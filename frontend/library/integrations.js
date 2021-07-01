import { defineAsyncComponent } from 'vue'
import { ElSkeleton } from 'element-plus'

export default [{
  label: 'Tableau',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/tableau.md'), loadingComponent: ElSkeleton}),
  logo: '/src/assets/images/integrations/tableau.png',
  category: 'BI Tools',
  beta: true
}, {
  label: 'Google Data Studio',
  logo: '/src/assets/images/integrations/google-datastudio.png',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/datastudio.md'), loadingComponent: ElSkeleton}),
  category: 'BI Tools',
  beta: true
}, {
  label: 'Google Sheets',
  logo: '/src/assets/images/integrations/google-sheets.png',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/googlesheets.md'), loadingComponent: ElSkeleton}),
  category: 'Sheets',
  ready: false
}, {
  label: 'Looker',
  logo: '/src/assets/images/integrations/looker.png',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/looker.md'), loadingComponent: ElSkeleton}),
  category: 'BI Tools',
  ready: false
},  {
  label: 'Apache Superset',
  category: 'BI Tools',
  logo: '/src/assets/images/integrations/superset.png',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/superset.md'), loadingComponent: ElSkeleton}),
}, {
  label: 'Metabase',
  category: 'BI Tools',
  logo: '/src/assets/images/integrations/metabase.png',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/metabase.md'), loadingComponent: ElSkeleton}),
}, {
  label: 'Python',
  category: 'Development',
  logo: '/src/assets/images/integrations/python.png',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/python.md'), loadingComponent: ElSkeleton}),
},{
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
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/jdbc.md'), loadingComponent: ElSkeleton}),
  category: 'BI Tools'
}].sort((a, b) => {
  return !b.ready
})
