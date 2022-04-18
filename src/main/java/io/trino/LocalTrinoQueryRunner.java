/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.jaxrs.testing.GuavaMultivaluedMap;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.client.ProtocolHeaders;
import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorManager;
import io.trino.connector.system.*;
import io.trino.connector.system.jdbc.*;
import io.trino.cost.*;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.*;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.resourcegroups.NoOpResourceGroupManager;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.index.IndexManager;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.*;
import io.trino.operator.*;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.security.GroupProviderManager;
import io.trino.server.HttpRequestSessionContext;
import io.trino.server.PluginManager;
import io.trino.server.QuerySessionSupplier;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorConfig;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.*;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.gen.*;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.*;
import io.trino.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.sanity.PlanSanityChecker;
import io.trino.sql.tree.*;
import io.trino.testing.MaterializedResult;
import io.trino.testing.PageConsumerOperator.PageConsumerOutputFactory;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingGroupProvider;
import io.trino.testing.TestingTaskContext;
import io.trino.transaction.InMemoryTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import io.trino.type.BlockTypeOperators;
import io.trino.util.FinalizerService;
import io.trino.util.StatementUtils;
import org.intellij.lang.annotations.Language;
import org.rakam.server.http.RakamHttpRequest;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.cost.StatsCalculatorModule.createNewStatsCalculator;
import static io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.version.EmbedVersion.testingVersionEmbedder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class LocalTrinoQueryRunner {
    public final SqlParser sqlParser;
    public final InMemoryNodeManager nodeManager;

    private final EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig());

    private final ExecutorService notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final FinalizerService finalizerService;
    private final PlanFragmenter planFragmenter;
    private final TypeOperators typeOperators;
    private final BlockTypeOperators blockTypeOperators;
    private final MetadataManager metadata;
    private final StatsCalculator statsCalculator;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final CostCalculator costCalculator;
    private final CostCalculator estimatedExchangesCostCalculator;
    private final TaskCountEstimator taskCountEstimator;
    private final TestingGroupProvider groupProvider;
    private final TestingAccessControlManager accessControl;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final CatalogManager catalogManager;
    private final TransactionManager transactionManager;
    private final FileSingleStreamSpillerFactory singleStreamSpillerFactory;
    private final SpillerFactory spillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private final PageFunctionCompiler pageFunctionCompiler;
    private final ExpressionCompiler expressionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final JoinCompiler joinCompiler;
    private final ConnectorManager connectorManager;
    private final PluginManager pluginManager;

    private final TaskManagerConfig taskManagerConfig;
    private final NodeSpillConfig nodeSpillConfig;
    private final List<PlanOptimizer> planOptimizers;
    private final ImmutableMap<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask;
    private final QuerySessionSupplier sessionSupplier;
    private final SessionPropertyManager sessionPropertyManager;

    public LocalTrinoQueryRunner(FeaturesConfig featuresConfig, NodeSpillConfig nodeSpillConfig) {
        this.taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        this.nodeSpillConfig = requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
        this.notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        this.yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
        this.finalizerService = new FinalizerService();
        finalizerService.start();

        this.typeOperators = new TypeOperators();
        this.blockTypeOperators = new BlockTypeOperators(typeOperators);
        this.sqlParser = new SqlParser();
        this.nodeManager = new InMemoryNodeManager();
        PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        this.indexManager = new IndexManager();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig().setIncludeCoordinator(true);
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, new NodeTaskMap(finalizerService)));
        this.pageSinkManager = new PageSinkManager();
        this.catalogManager = new CatalogManager();
        this.transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                yieldExecutor,
                catalogManager,
                notificationExecutor);
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler, blockTypeOperators);

        this.sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(new QueryManagerConfig(), taskManagerConfig, new MemoryManagerConfig(), featuresConfig, new NodeMemoryConfig(), new DynamicFilterConfig(), new NodeSchedulerConfig()));
        this.metadata = new MetadataManager(
                featuresConfig,
                sessionPropertyManager,
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new MaterializedViewPropertyManager(),
                new ColumnPropertyManager(),
                new AnalyzePropertyManager(),
                transactionManager,
                typeOperators,
                blockTypeOperators,
                nodeManager.getCurrentNode().getNodeVersion());
        this.splitManager = new SplitManager(new QueryManagerConfig(), metadata);
        this.planFragmenter = new PlanFragmenter(this.metadata, this.nodePartitioningManager, new QueryManagerConfig());
        this.joinCompiler = new JoinCompiler(typeOperators);
        PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler, blockTypeOperators);
        this.statsCalculator = createNewStatsCalculator(metadata, new TypeAnalyzer(sqlParser, metadata));
        this.scalarStatsCalculator = new ScalarStatsCalculator(metadata, new TypeAnalyzer(sqlParser, metadata));
        this.taskCountEstimator = new TaskCountEstimator(() -> 1);
        this.costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        this.estimatedExchangesCostCalculator = new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator);
        this.groupProvider = new TestingGroupProvider();
        this.accessControl = new TestingAccessControlManager(transactionManager, eventListenerManager);
        accessControl.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        this.pageSourceManager = new PageSourceManager();

        this.pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);
        this.expressionCompiler = new ExpressionCompiler(metadata, pageFunctionCompiler);
        this.joinFilterFunctionCompiler = new JoinFilterFunctionCompiler(metadata);

        NodeInfo nodeInfo = new NodeInfo("test");
        this.connectorManager = new ConnectorManager(
                metadata,
                catalogManager,
                accessControl,
                splitManager,
                pageSourceManager,
                indexManager,
                nodePartitioningManager,
                pageSinkManager,
                new HandleResolver(),
                nodeManager,
                nodeInfo,
                testingVersionEmbedder(),
                pageSorter,
                pageIndexerFactory,
                transactionManager,
                eventListenerManager,
                typeOperators,
                nodeSchedulerConfig);

        GlobalSystemConnectorFactory globalSystemConnectorFactory = new GlobalSystemConnectorFactory(ImmutableSet.of(
                new NodeSystemTable(nodeManager),
                new CatalogSystemTable(metadata, accessControl),
                new TableCommentSystemTable(metadata, accessControl),
                new SchemaPropertiesSystemTable(transactionManager, metadata),
                new TablePropertiesSystemTable(transactionManager, metadata),
                new MaterializedViewPropertiesSystemTable(transactionManager, metadata),
                new ColumnPropertiesSystemTable(transactionManager, metadata),
                new AnalyzePropertiesSystemTable(transactionManager, metadata),
                new TransactionsSystemTable(metadata, transactionManager),

                // should match io.trino.connector.system.SystemConnectorModule JDBC tables
                new AttributeJdbcTable(),
                new CatalogJdbcTable(metadata, accessControl),
                new ColumnJdbcTable(metadata, accessControl),
                new TypesJdbcTable(metadata),
                new ProcedureColumnJdbcTable(),
                new ProcedureJdbcTable(),
                new PseudoColumnJdbcTable(),
                new SchemaJdbcTable(metadata, accessControl),
                new SuperTableJdbcTable(),
                new SuperTypeJdbcTable(),
                new TableJdbcTable(metadata, accessControl),
                new TableTypeJdbcTable(),
                new UdtJdbcTable()
        ), ImmutableSet.of());

        this.pluginManager = new PluginManager(
                (loader, createClassLoader) -> {
                },
                connectorManager,
                metadata,
                new NoOpResourceGroupManager(),
                accessControl,
                Optional.of(new PasswordAuthenticatorManager(new PasswordAuthenticatorConfig())),
                new CertificateAuthenticatorManager(),
                eventListenerManager,
                new GroupProviderManager(),
                new SessionPropertyDefaults(nodeInfo));

        connectorManager.addConnectorFactory(globalSystemConnectorFactory, globalSystemConnectorFactory.getClass()::getClassLoader);
        connectorManager.createCatalog(GlobalSystemConnector.NAME, GlobalSystemConnector.NAME, ImmutableMap.of());

        SpillerStats spillerStats = new SpillerStats();
        this.singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(metadata, spillerStats, featuresConfig, nodeSpillConfig);
        this.partitioningSpillerFactory = new GenericPartitioningSpillerFactory(this.singleStreamSpillerFactory);
        this.spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);

        this.planOptimizers = new PlanOptimizers(
                metadata,
                typeOperators,
                new TypeAnalyzer(sqlParser, metadata),
                taskManagerConfig,
                true,
                splitManager,
                pageSourceManager,
                statsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                new CostComparator(featuresConfig),
                taskCountEstimator,
                nodePartitioningManager).get();

        sessionSupplier = new QuerySessionSupplier(transactionManager, accessControl, sessionPropertyManager, new SqlEnvironmentConfig());

        dataDefinitionTask = ImmutableMap.<Class<? extends Statement>, DataDefinitionTask<?>>builder()
                .put(Use.class, new UseTask())
                .put(Comment.class, new CommentTask())
                .put(Prepare.class, new PrepareTask(sqlParser))
                .put(Deallocate.class, new DeallocateTask())
                .put(ResetSession.class, new ResetSessionTask())
                .put(SetSession.class, new SetSessionTask())
                .put(StartTransaction.class, new StartTransactionTask())
                .build();
    }

    public void close() {
        notificationExecutor.shutdownNow();
        yieldExecutor.shutdownNow();
        connectorManager.stop();
        finalizerService.destroy();
        singleStreamSpillerFactory.destroy();
    }

    public void addSystemProperty(PropertyMetadata propertyMetadata) {
        sessionPropertyManager.addSystemSessionProperty(propertyMetadata);
    }

    public void createCatalog(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties) {
        nodeManager.addCurrentNodeConnector(new CatalogName(catalogName));
        connectorManager.addConnectorFactory(connectorFactory, connectorFactory.getClass()::getClassLoader);
        connectorManager.createCatalog(catalogName, connectorFactory.getName(), properties);
    }

    public void installPlugin(Plugin plugin) {
        pluginManager.installPlugin(plugin, plugin.getClass()::getClassLoader);
    }

    public MaterializedResultWithPlanHeader executeWithPlan(UUID queryId, HttpRequestSessionContext sessionContext, String sql, WarningCollector warningCollector) {
        String queryIdForTrino = queryId.toString().replace('-', '_');
        Session session = sessionSupplier.createSession(QueryId.valueOf(queryIdForTrino), sessionContext);
        return inTransaction(session, transactionSession -> executeInternal(transactionSession, sql, warningCollector));
    }

    public <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer) {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    private MaterializedResultWithPlanHeader executeInternal(Session session, @Language("SQL") String sql, WarningCollector warningCollector) {
        try (Closer closer = Closer.create()) {
            AtomicReference<MaterializedResult.Builder> builder = new AtomicReference<>();

            TaskContext taskContext = TestingTaskContext.builder(notificationExecutor, yieldExecutor, session)
                    .setMaxSpillSize(nodeSpillConfig.getMaxSpillPerNode())
                    .setQueryMaxSpillSize(nodeSpillConfig.getQueryMaxSpillPerNode())
                    .build();

            PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

            Class<? extends Statement> statementClazz = preparedQuery.getStatement().getClass();
            QueryType queryType = StatementUtils.getQueryType(statementClazz).orElse(null);

            AtomicReference<Map<String, String>> headers = new AtomicReference<>();
            if (queryType == QueryType.DATA_DEFINITION) {
                DataDefinitionTask<? super Statement> task = (DataDefinitionTask<? super Statement>) this.dataDefinitionTask.get(statementClazz);
                // we don't need else statement as the relevant exception is thrown by the StatementAnalyzer
                if (task != null) {
                    QueryStateMachine stateMachine = QueryStateMachine.begin(
                            sql,
                            preparedQuery.getPrepareSql(),
                            session,
                            URI.create("fake://uri"),
                            new ResourceGroupId("test"),
                            false,
                            transactionManager,
                            accessControl,
                            MoreExecutors.directExecutor(),
                            metadata,
                            warningCollector,
                            Optional.ofNullable(queryType));
                    stateMachine.setUpdateType(task.getName());

                    ListenableFuture<?> result = task.execute(preparedQuery.getStatement(), transactionManager, metadata, accessControl, stateMachine, preparedQuery.getParameters(), warningCollector);
                    result.get();
                    QueryInfo queryInfo = stateMachine.updateQueryInfo(Optional.empty());
                    headers.compareAndSet(null, getResponseHeaders(queryInfo, stateMachine.getSession()));

                    MaterializedResult.Builder materializedResult = MaterializedResult.resultBuilder(session, ImmutableList.of(BooleanType.BOOLEAN));
                    builder.compareAndSet(null, materializedResult);
                    materializedResult.row(true);

                    return new MaterializedResultWithPlanHeader(materializedResult.build(), null, headers.get(), queryInfo.getUpdateType());
                } else {
                    throw new UnsupportedOperationException(String.format("Invalid task %s", statementClazz));
                }
            } else if (queryType == QueryType.SELECT || queryType == QueryType.ANALYZE || queryType == QueryType.DESCRIBE || queryType == QueryType.EXPLAIN) {
                Plan plan = createPlan(session, preparedQuery, warningCollector);

                PageConsumerOutputFactory outputFactory = new PageConsumerOutputFactory(types -> {
                    builder.compareAndSet(null, MaterializedResult.resultBuilder(session, types));
                    return builder.get()::page;
                });
                List<Driver> drivers = createDrivers(session, plan, outputFactory, taskContext);
                drivers.forEach(closer::register);

                boolean done = false;
                while (!done) {
                    boolean processed = false;
                    for (Driver driver : drivers) {
                        if (!driver.isFinished()) {
                            driver.process();
                            processed = true;
                        }
                    }
                    done = !processed;
                }

                MaterializedResult.Builder builderValue = builder.get();
                verify(builderValue != null, "Output operator was not created");
                return new MaterializedResultWithPlanHeader(builderValue.build(), plan, headers.get(), null);
            } else {
                throw new UnsupportedOperationException(String.format("%s operation is not supported in metriql", queryType));
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, UTF_8);
    }

    private static Map<String, String> getResponseHeaders(QueryInfo info, Session session) {
        Map<String, String> headers = new HashMap<>();
        ProtocolHeaders protocolHeaders = session.getProtocolHeaders();
        info.getSetCatalog().ifPresent(catalog -> headers.put(protocolHeaders.responseSetCatalog(), catalog));
        info.getSetSchema().ifPresent(schema -> headers.put(protocolHeaders.responseSetSchema(), schema));
        info.getSetPath().ifPresent(path -> headers.put(protocolHeaders.responseSetPath(), path));

        // add set session properties
        info.getSetSessionProperties()
                .forEach((key, value) -> headers.put(protocolHeaders.responseSetSession(), key + '=' + urlEncode(value)));

        // add clear session properties
        info.getResetSessionProperties()
                .forEach(name -> headers.put(protocolHeaders.responseClearSession(), name));

        // add set roles
        info.getSetRoles()
                .forEach((key, value) -> headers.put(protocolHeaders.responseSetRole(), key + '=' + urlEncode(value.toString())));

        // add added prepare statements
        for (Map.Entry<String, String> entry : info.getAddedPreparedStatements().entrySet()) {
            String encodedKey = urlEncode(entry.getKey());
            String encodedValue = urlEncode(entry.getValue());
            headers.put(protocolHeaders.responseAddedPrepare(), encodedKey + '=' + encodedValue);
        }

        // add deallocated prepare statements
        for (String name : info.getDeallocatedPreparedStatements()) {
            headers.put(protocolHeaders.responseDeallocatedPrepare(), urlEncode(name));
        }

        // add new transaction ID
        info.getStartedTransactionId()
                .ifPresent(transactionId -> headers.put(protocolHeaders.responseStartedTransactionId(), transactionId.toString()));

        // add clear transaction ID directive
        if (info.isClearTransactionId()) {
            headers.put(protocolHeaders.responseClearTransactionId(), "true");
        }

        return headers;
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode) {
        return planFragmenter.createSubPlans(session, plan, forceSingleNode, WarningCollector.NOOP);
    }

    private List<Driver> createDrivers(Session session, Plan plan, OutputFactory outputFactory, TaskContext taskContext) {
        SubPlan subplan = createSubPlans(session, plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected subplan to have no children");
        }

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                metadata,
                new TypeAnalyzer(sqlParser, metadata),
                Optional.empty(),
                pageSourceManager,
                indexManager,
                nodePartitioningManager,
                pageSinkManager,
                null,
                expressionCompiler,
                pageFunctionCompiler,
                joinFilterFunctionCompiler,
                new IndexJoinLookupStats(),
                this.taskManagerConfig,
                spillerFactory,
                singleStreamSpillerFactory,
                partitioningSpillerFactory,
                new PagesIndex.TestingFactory(false),
                joinCompiler,
                new LookupJoinOperators(),
                new OrderingCompiler(typeOperators),
                new DynamicFilterConfig(),
                typeOperators,
                blockTypeOperators);

        // plan query
        StageExecutionDescriptor stageExecutionDescriptor = subplan.getFragment().getStageExecutionDescriptor();
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                stageExecutionDescriptor,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getPartitioningScheme().getOutputLayout(),
                plan.getTypes(),
                subplan.getFragment().getPartitionedSources(),
                outputFactory);

        // generate sources
        List<TaskSource> sources = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            TableHandle table = tableScan.getTable();

            SplitSource splitSource = splitManager.getSplits(
                    session,
                    table,
                    stageExecutionDescriptor.isScanGroupedExecution(tableScan.getId()) ? GROUPED_SCHEDULING : UNGROUPED_SCHEDULING,
                    EMPTY);

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                for (Split split : getNextBatch(splitSource)) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScan.getId(), split));
                }
            }

            sources.add(new TaskSource(tableScan.getId(), scheduledSplits.build(), true));
        }

        // create drivers
        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, DriverFactory> driverFactoriesBySource = new HashMap<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            for (int i = 0; i < driverFactory.getDriverInstances().orElse(1); i++) {
                if (driverFactory.getSourceId().isPresent()) {
                    checkState(driverFactoriesBySource.put(driverFactory.getSourceId().get(), driverFactory) == null);
                } else {
                    DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), false).addDriverContext();
                    Driver driver = driverFactory.createDriver(driverContext);
                    drivers.add(driver);
                }
            }
        }

        // add sources to the drivers
        ImmutableSet<PlanNodeId> partitionedSources = ImmutableSet.copyOf(subplan.getFragment().getPartitionedSources());
        for (TaskSource source : sources) {
            DriverFactory driverFactory = driverFactoriesBySource.get(source.getPlanNodeId());
            checkState(driverFactory != null);
            boolean partitioned = partitionedSources.contains(driverFactory.getSourceId().get());
            for (ScheduledSplit split : source.getSplits()) {
                DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned).addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                driver.updateSource(new TaskSource(split.getPlanNodeId(), ImmutableSet.of(split), true));
                drivers.add(driver);
            }
        }

        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            driverFactory.noMoreDrivers();
        }

        return ImmutableList.copyOf(drivers);
    }

    public Plan createPlan(Session session, PreparedQuery preparedQuery, WarningCollector warningCollector) {

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        QueryExplainer queryExplainer = new QueryExplainer(
                planOptimizers,
                planFragmenter,
                metadata,
                typeOperators,
                groupProvider,
                accessControl,
                sqlParser,
                statsCalculator,
                costCalculator,
                dataDefinitionTask);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, groupProvider, accessControl, Optional.of(queryExplainer), preparedQuery.getParameters(), parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()), warningCollector, statsCalculator);

        LogicalPlanner logicalPlanner = new LogicalPlanner(
                session,
                planOptimizers,
                new PlanSanityChecker(true),
                idAllocator,
                metadata,
                typeOperators,
                new TypeAnalyzer(sqlParser, metadata),
                statsCalculator,
                costCalculator,
                warningCollector);

        Analysis analysis = analyzer.analyze(preparedQuery.getStatement());
        // make LocalQueryRunner always compute plan statistics for test purposes
        return logicalPlanner.plan(analysis, OPTIMIZED_AND_VALIDATED);
    }

    private static List<Split> getNextBatch(SplitSource splitSource) {
        return getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000)).getSplits();
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node) {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    public class MaterializedResultWithPlanHeader {
        public final MaterializedResult materializedResult;
        public final Plan queryPlan;
        public final Map<String, String> responseHeaders;
        public final String updateType;

        public MaterializedResultWithPlanHeader(MaterializedResult materializedResult, Plan queryPlan, Map<String, String> responseHeaders, String updateType) {
            this.materializedResult = materializedResult;
            this.queryPlan = queryPlan;
            this.responseHeaders = responseHeaders;
            this.updateType = updateType;
        }
    }
}
