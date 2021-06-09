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
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
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
import io.trino.server.PluginManager;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorConfig;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.*;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
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
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.sql.planner.sanity.PlanSanityChecker;
import io.trino.sql.tree.*;
import io.trino.testing.*;
import io.trino.testing.PageConsumerOperator.PageConsumerOutputFactory;
import io.trino.transaction.InMemoryTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import io.trino.type.BlockTypeOperators;
import io.trino.util.FinalizerService;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
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
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.testing.TreeAssertions.assertFormattedSql;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.version.EmbedVersion.testingVersionEmbedder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class LocalQueryRunner
        implements QueryRunner {
    private final EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig());

    private final Session defaultSession;
    private final ExecutorService notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final FinalizerService finalizerService;

    private final SqlParser sqlParser;
    private final PlanFragmenter planFragmenter;
    private final InMemoryNodeManager nodeManager;
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
    private final boolean alwaysRevokeMemory;
    private final NodeSpillConfig nodeSpillConfig;
    private final FeaturesConfig featuresConfig;
    private final List<PlanOptimizer> planOptimizers;

//    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static LocalQueryRunner create(Session defaultSession) {
        return builder(defaultSession).build();
    }

    public static Builder builder(Session defaultSession) {
        return new Builder(defaultSession);
    }

    private LocalQueryRunner(
            Session defaultSession,
            FeaturesConfig featuresConfig,
            NodeSpillConfig nodeSpillConfig,
            boolean withInitialTransaction,
            boolean alwaysRevokeMemory,
            int nodeCountForStats,
            Map<String, List<PropertyMetadata<?>>> defaultSessionProperties) {
        requireNonNull(defaultSession, "defaultSession is null");
        requireNonNull(defaultSessionProperties, "defaultSessionProperties is null");
        checkArgument(defaultSession.getTransactionId().isEmpty() || !withInitialTransaction, "Already in transaction");

        this.taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        this.nodeSpillConfig = requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
        this.alwaysRevokeMemory = alwaysRevokeMemory;
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
        this.featuresConfig = requireNonNull(featuresConfig, "featuresConfig is null");
        this.pageSinkManager = new PageSinkManager();
        this.catalogManager = new CatalogManager();
        this.transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                yieldExecutor,
                catalogManager,
                notificationExecutor);
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler, blockTypeOperators);

        this.metadata = new MetadataManager(
                featuresConfig,
                new SessionPropertyManager(new SystemSessionProperties(new QueryManagerConfig(), taskManagerConfig, new MemoryManagerConfig(), featuresConfig, new NodeMemoryConfig(), new DynamicFilterConfig(), new NodeSchedulerConfig())),
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
        this.taskCountEstimator = new TaskCountEstimator(() -> nodeCountForStats);
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
        ),
                ImmutableSet.of());

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

        // rewrite session to use managed SessionPropertyMetadata
        this.defaultSession = new Session(
                defaultSession.getQueryId(),
                withInitialTransaction ? Optional.of(transactionManager.beginTransaction(false)) : defaultSession.getTransactionId(),
                defaultSession.isClientTransactionSupport(),
                defaultSession.getIdentity(),
                defaultSession.getSource(),
                defaultSession.getCatalog(),
                defaultSession.getSchema(),
                defaultSession.getPath(),
                defaultSession.getTraceToken(),
                defaultSession.getTimeZoneKey(),
                defaultSession.getLocale(),
                defaultSession.getRemoteUserAddress(),
                defaultSession.getUserAgent(),
                defaultSession.getClientInfo(),
                defaultSession.getClientTags(),
                defaultSession.getClientCapabilities(),
                defaultSession.getResourceEstimates(),
                defaultSession.getStart(),
                defaultSession.getSystemProperties(),
                defaultSession.getConnectorProperties(),
                defaultSession.getUnprocessedCatalogProperties(),
                metadata.getSessionPropertyManager(),
                defaultSession.getPreparedStatements(),
                defaultSession.getProtocolHeaders());

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
    }

    @Override
    public void close() {
        notificationExecutor.shutdownNow();
        yieldExecutor.shutdownNow();
        connectorManager.stop();
        finalizerService.destroy();
        singleStreamSpillerFactory.destroy();
    }

    public void loadEventListeners() {
        this.eventListenerManager.loadEventListeners();
    }

    @Override
    public int getNodeCount() {
        return 1;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    @Override
    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public SqlParser getSqlParser() {
        return sqlParser;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    public TypeOperators getTypeOperators() {
        return typeOperators;
    }

    public BlockTypeOperators getBlockTypeOperators() {
        return blockTypeOperators;
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager() {
        return nodePartitioningManager;
    }

    @Override
    public PageSourceManager getPageSourceManager() {
        return pageSourceManager;
    }

    @Override
    public SplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public StatsCalculator getStatsCalculator() {
        return statsCalculator;
    }

    @Override
    public TestingGroupProvider getGroupProvider() {
        return groupProvider;
    }

    @Override
    public TestingAccessControlManager getAccessControl() {
        return accessControl;
    }

    public ExecutorService getExecutor() {
        return notificationExecutor;
    }

    public ScheduledExecutorService getScheduler() {
        return yieldExecutor;
    }

    @Override
    public Session getDefaultSession() {
        return defaultSession;
    }

    public ExpressionCompiler getExpressionCompiler() {
        return expressionCompiler;
    }

    public void createCatalog(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties) {
        nodeManager.addCurrentNodeConnector(new CatalogName(catalogName));
        connectorManager.addConnectorFactory(connectorFactory, connectorFactory.getClass()::getClassLoader);
        connectorManager.createCatalog(catalogName, connectorFactory.getName(), properties);
    }

    @Override
    public void installPlugin(Plugin plugin) {
        pluginManager.installPlugin(plugin, plugin.getClass()::getClassLoader);
    }

    @Override
    public void addFunctions(List<? extends SqlFunction> functions) {
        metadata.addFunctions(functions);
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties) {
        nodeManager.addCurrentNodeConnector(new CatalogName(catalogName));
        connectorManager.createCatalog(catalogName, connectorName, properties);
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql) {
        return execute(defaultSession, sql);
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql) {
        return executeWithPlan(session, sql, WarningCollector.NOOP).getMaterializedResult();
    }

    public MaterializedResultWithPlan executeWithPlan(String sql) {
        return inTransaction(defaultSession, transactionSession -> executeInternal(transactionSession, sql));
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql, WarningCollector warningCollector) {
        return inTransaction(session, transactionSession -> executeInternal(transactionSession, sql));
    }

    public <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer) {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    private MaterializedResultWithPlan executeInternal(Session session, @Language("SQL") String sql) {
        try (Closer closer = Closer.create()) {
            AtomicReference<MaterializedResult.Builder> builder = new AtomicReference<>();
            PageConsumerOutputFactory outputFactory = new PageConsumerOutputFactory(types -> {
                builder.compareAndSet(null, MaterializedResult.resultBuilder(session, types));
                return builder.get()::page;
            });

            TaskContext taskContext = TestingTaskContext.builder(notificationExecutor, yieldExecutor, session)
                    .setMaxSpillSize(nodeSpillConfig.getMaxSpillPerNode())
                    .setQueryMaxSpillSize(nodeSpillConfig.getQueryMaxSpillPerNode())
                    .build();

            Plan plan = createPlan(session, sql, WarningCollector.NOOP);
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

            verify(builder.get() != null, "Output operator was not created");
            return new MaterializedResultWithPlan(builder.get().build(), plan);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    @Override
    public Lock getExclusiveLock() {
        throw new UnsupportedOperationException();
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

    @Override
    public Plan createPlan(Session session, @Language("SQL") String sql, WarningCollector warningCollector) {
        return createPlan(session, sql, OPTIMIZED_AND_VALIDATED, warningCollector);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(Session session, String table) {
        throw new UnsupportedOperationException();
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, WarningCollector warningCollector) {
        return createPlan(session, sql, stage, true, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, WarningCollector warningCollector) {
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());

        return createPlan(session, sql, planOptimizers, stage, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage, WarningCollector warningCollector) {
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        QueryExplainer queryExplainer = new QueryExplainer(
                optimizers,
                planFragmenter,
                metadata,
                typeOperators,
                groupProvider,
                accessControl,
                sqlParser,
                statsCalculator,
                costCalculator,
                new HashMap<>());
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, groupProvider, accessControl, Optional.of(queryExplainer), preparedQuery.getParameters(), parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()), warningCollector, statsCalculator);

        LogicalPlanner logicalPlanner = new LogicalPlanner(
                session,
                optimizers,
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
        return logicalPlanner.plan(analysis, stage);
    }

    private static List<Split> getNextBatch(SplitSource splitSource) {
        return getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000)).getSplits();
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node) {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    public static class Builder {
        private final Session defaultSession;
        private FeaturesConfig featuresConfig = new FeaturesConfig();
        private NodeSpillConfig nodeSpillConfig = new NodeSpillConfig();
        private boolean initialTransaction;
        private boolean alwaysRevokeMemory;
        private Map<String, List<PropertyMetadata<?>>> defaultSessionProperties = ImmutableMap.of();
        private int nodeCountForStats;

        private Builder(Session defaultSession) {
            this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        }

        public Builder withDefaultSessionProperties(Map<String, List<PropertyMetadata<?>>> defaultSessionProperties) {
            this.defaultSessionProperties = requireNonNull(defaultSessionProperties, "defaultSessionProperties is null");
            return this;
        }

        public LocalQueryRunner build() {
            return new LocalQueryRunner(
                    defaultSession,
                    featuresConfig,
                    nodeSpillConfig,
                    initialTransaction,
                    alwaysRevokeMemory,
                    nodeCountForStats,
                    defaultSessionProperties);
        }
    }
}
