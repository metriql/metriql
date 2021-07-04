import { defineAsyncComponent } from 'vue'
import { ElSkeleton } from 'element-plus'
import tableauLogo from '/src/assets/images/integrations/tableau.png'
import dataStudioLogo from '/src/assets/images/integrations/google-datastudio.png'
import googleSheetsLogo from '/src/assets/images/integrations/google-sheets.png'
import lookerLogo from '/src/assets/images/integrations/looker.png'
import supersetLogo from '/src/assets/images/integrations/superset.png'
import metabaseLogo from '/src/assets/images/integrations/metabase.png'
import pythonLogo from '/src/assets/images/integrations/python.png'

export default [{
  label: 'Tableau',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/tableau.md'), loadingComponent: ElSkeleton}),
  logo: tableauLogo,
  category: 'BI Tools',
  beta: true
}, {
  label: 'Google Data Studio',
  logo: dataStudioLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/datastudio.md'), loadingComponent: ElSkeleton}),
  category: 'BI Tools',
  beta: true
}, {
  label: 'Google Sheets',
  logo: googleSheetsLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/googlesheets.md'), loadingComponent: ElSkeleton}),
  category: 'Sheets',
  ready: false
}, {
  label: 'Looker',
  logo: lookerLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/looker.md'), loadingComponent: ElSkeleton}),
  category: 'BI Tools',
  ready: false
}, {
  label: 'Apache Superset',
  category: 'BI Tools',
  logo: supersetLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/superset.md'), loadingComponent: ElSkeleton}),
}, {
  label: 'Metabase',
  category: 'BI Tools',
  logo: metabaseLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/metabase.md'), loadingComponent: ElSkeleton}),
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
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/jdbc.md'), loadingComponent: ElSkeleton}),
  category: 'BI Tools'
}].sort((a, b) => {
  return !b.ready
})
