<template>
  <main>
    <v-chart ref="queryChart" class="chart" :option="chartOptions" manual-update/>
    <div class="columns">
      <div class="column"><h3 style="vertical-align:middle">Active Queries</h3></div>
      <div class="column is-narrow">
        <el-input type="search" prefix-icon="el-icon-search" placeholder="type to search in queries"
                  v-model="searchTerm" style="width:250px"></el-input>
      </div>
      <div class="column is-narrow">
        <el-radio-group v-model="activeState">
          <el-radio-button v-for="(state, key) in states" :label="key" :key="key">{{ key }}</el-radio-button>
        </el-radio-group>
      </div>
    </div>

    <el-table
      v-loading="filteredTableData == null"
      :data="filteredTableData || []"
      empty-text="Looks like the server just started?"
      height="450">
      <el-table-column label="Status" width="100">
        <template #default="scope">
          <el-tag size="small" :type="states[scope.row.status].type">{{ scope.row.status }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Started at" width="230">
        <template #default="scope">
          {{ formatDate(scope.row.startedAt) }}
        </template>
      </el-table-column>
      <el-table-column label="User" prop="user" width="200" :filters="distinctUsers"
                       :filter-method="(value, row) => row.user == value"></el-table-column>
      <el-table-column label="Source" prop="source" width="200" :filters="distinctSources"
                       :filter-method="(value, row) => row.source == value"></el-table-column>
      <el-table-column label="Type" width="150">
        <template #default="scope">
            {{ scope.row.update?.info?.reportType }}
            {{ scope.row.update }}
        </template>
      </el-table-column>
      <el-table-column label="Query">
        <template #default="scope">
          <a @click="activeTask = scope.row">
            {{ getTrimmedQuery(scope.row) }}
          </a>
        </template>
      </el-table-column>
      <el-table-column width="140">
        <template #default="scope">
          {{ getDurationOfTask(scope.row.duration) }}
          <el-button v-if="scope.row.update.state == 'RUNNING'" type="danger" plain size="small">Kill</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog
      v-if="activeTask != null"
      :title="activeTask.update?.info?.reportType"
      :modelValue="activeTask != null"
      @update:modelValue="$event == false ? (activeTask = null) : null"
      width="50%">
      <pre>{{ activeTask.update?.info?.query?.query }}</pre>
      <h4 style="margin:15px 0">Compiled:</h4>
      <pre>{{ activeTask.update?.info?.compiledQuery }}</pre>
      <template #footer>
          <span class="dialog-footer">
            <el-button @click="activeTask = null">OK</el-button>
          </span>
      </template>
    </el-dialog>
  </main>
</template>
<script>
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TitleComponent, TooltipComponent } from 'echarts/components'
import VChart, { THEME_KEY } from 'vue-echarts'
import { computed, onMounted, ref } from 'vue'
import { MetriqlAdmin } from '../services/MetriqlAdmin'
import format from 'date-fns/format'
import LiveStatistics from '/src/services/live-statistics'
import TableLite from "vue3-table-lite";

use([
  CanvasRenderer,
  LineChart,
  GridComponent,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
])

const states = {
  'queued': {type: 'info'},
  'running': {type: 'warning'},
  'canceled': {type: 'error'},
  'finished': {type: 'success'},
}

const getFilters = function (tableData, property) {
  if (tableData == null) {
    return []
  }
  const values = new Set()
  tableData.forEach(row => {
    values.add(row[property])
  })
  return [...values].map(value => ({text: value, value: value}))
}

export default {
  name: 'Monitoring',
  components: {
    VChart, TableLite
  },
  provide: {
    [THEME_KEY]: 'light'
  },
  computed: {
    columns: function() {
      return [{
        label: "Status",
        field: "status",
        width: "100px",
        sortable: true,
        display: function (row) {
          return (
            `<span class="el-tag el-tag--${states[row.status].type} el-tag--small el-tag--light">${row.status}</span>`
          );
        },
      }, {
        label: "Started at",
        field: "startedAt",
        width: "230px",
        sortable: true,
        display: function (row) {
          return this.formatDate(row.startedAt);
        },
      }]
    },
    distinctSources: function () {
      return getFilters(this.filteredTableData, 'source')
    },
    distinctUsers: function () {
      return getFilters(this.filteredTableData, 'user')
    }
  },
  methods: {
    formatDate: function (date) {
      return format(new Date(date), 'yyyy-MM-dd HH:mm:ss')
    },
    getDurationOfTask: function (duration) {
      const sec_num = parseInt(duration, 10) // don't forget the second param
      let hours = Math.floor(sec_num / 3600)
      let minutes = Math.floor((sec_num - (hours * 3600)) / 60)
      let seconds = sec_num - (hours * 3600) - (minutes * 60)

      let hourPrefix
      if (hours == 0) {
        hourPrefix = ''
      } else if (hours < 10) {
        hourPrefix = `0${hours}:`
      } else {
        hourPrefix = `${hours}:`
      }
      return hourPrefix
        + (minutes < 10 ? `0${minutes}` : minutes) + ':'
        + (seconds < 10 ? `0${seconds}` : seconds)
    },
    getTrimmedQuery: function (task) {
      let query = task.update.info?.compiledQuery || ''
      if (query.length > 100) {
        return query.substring(0, 100)
      } else {
        return query
      }
    }
  },
  setup () {
    const searchTerm = ref('')
    const activeState = ref(null)
    const tableData = ref(null)
    const activeTask = ref(null)

    const chartData = LiveStatistics.points.map(row => ({
      name: row[0].toString(),
      value: row
    }))

    const chartOptions = {
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          animation: false
        }
      },
      xAxis: {
        type: 'time',
        splitLine: {
          show: false
        },
        axisPointer: {
          snap: true,
          handle: {
            show: true,
            color: 'red'
          }
        }
      },
      yAxis: {
        type: 'value',
        boundaryGap: [0, '100%'],
        splitLine: {
          show: false
        }
      },
      dataZoom: [{
          type: 'inside',
          start: 0,
          end: 20
        },
        {
          start: 0,
          end: 20
        }
      ],
      series: [{
        name: 'Number of queries',
        type: 'line',
        showSymbol: false,
        hoverAnimation: false,
        data: chartData
      }]
    }

    const updateChart = (chart, row) => {
      chartData.push({
        name: row[0].toString(),
        value: row
      })

      if (chart.value) {
        chart.value.setOption({
          series: [{
            data: chartData
          }]
        })
      }
    }

    const queryChart = ref(null)

    onMounted(() => {
      LiveStatistics.register(row => {
        updateChart(queryChart, row)
        MetriqlAdmin.getTasks({params: {status: activeState.value}}).then(data => {
          tableData.value = data
        })
      })
    })

    const filteredTableData = computed(() => {
      let hasSearchTerm = searchTerm.value != ''
      if (hasSearchTerm && tableData != null) {
        return tableData.value.filter(task => {
          return task.update?.query?.toLowerCase()?.includes(searchTerm.value)
        })
      } else {
        return tableData.value
      }
    })

    console.log(filteredTableData)
    return {activeState, filteredTableData, states, chartOptions, queryChart, searchTerm, chartData, activeTask}
  }
}
</script>
<style scoped>
.chart {
  height: 200px;
}
</style>
