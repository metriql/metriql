import { defineAsyncComponent } from 'vue'
import { ElSkeleton } from 'element-plus'
import tableauLogo from '/src/assets/images/integrations/tableau.png'
import dataStudioLogo from '/src/assets/images/integrations/google-datastudio.png'
import googleSheetsLogo from '/src/assets/images/integrations/google-sheets.png'
import lookerLogo from '/src/assets/images/integrations/looker.png'
import supersetLogo from '/src/assets/images/integrations/superset.png'
import metabaseLogo from '/src/assets/images/integrations/metabase.png'
import pythonLogo from '/src/assets/images/integrations/python.png'
import {request} from '/src/services/request'

export default [{
  label: 'Tableau',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/tableau.vue'), loadingComponent: ElSkeleton}),
  logo: tableauLogo,
  category: 'BI Tools',
  action: function(dataset) {
    return `api/v0/integration/tableau?dataset=${dataset}`
  },
  source: 'https://github.com/metriql/metriql-tableau',
  beta: true
}, {
  label: 'Google Data Studio',
  logo: dataStudioLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/datastudio.vue'), loadingComponent: ElSkeleton}),
  category: 'BI Tools',
  beta: true
},  {
  label: 'Looker',
  logo: lookerLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/looker.vue'), loadingComponent: ElSkeleton}),
  source: 'https://github.com/metriql/metriql-lookml',
  action: function(connection) {
    return request.post('api/v0/integration/looker', {connection})
  },
  category: 'BI Tools',
}, {
  label: 'Google Sheets',
  logo: googleSheetsLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/googlesheets.md'), loadingComponent: ElSkeleton}),
  category: 'Sheets',
  ready: false
}, {
  label: 'Apache Superset',
  category: 'BI Tools',
  logo: supersetLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/superset.md'), loadingComponent: ElSkeleton}),
  ready: false,
}, {
  label: 'Metabase',
  category: 'BI Tools',
  logo: metabaseLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/metabase.md'), loadingComponent: ElSkeleton}),
  ready: false,
}, {
  label: 'Python',
  category: 'Development',
  logo: pythonLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/python.md'), loadingComponent: ElSkeleton}),
}, {
  label: 'REST API',
  category: 'Development',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/rest.vue'), loadingComponent: ElSkeleton}),
  full: true
}, {
  label: 'CLI',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/cli.md'), loadingComponent: ElSkeleton}),
  category: 'Development'
}, {
  label: 'JDBC Driver',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/jdbc.vue'), loadingComponent: ElSkeleton}),
  category: 'BI Tools'
}].sort((a, b) => {
  return !b.ready
})
