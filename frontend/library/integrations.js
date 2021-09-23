import { defineAsyncComponent } from 'vue'
import { ElSkeleton } from 'element-plus'
import tableauLogo from '/src/assets/images/integrations/tableau.png'
import dataStudioLogo from '/src/assets/images/integrations/google-datastudio.png'
import googleSheetsLogo from '/src/assets/images/integrations/google-sheets.png'
import lookerLogo from '/src/assets/images/integrations/looker.png'
import supersetLogo from '/src/assets/images/integrations/superset.png'
import metabaseLogo from '/src/assets/images/integrations/metabase.png'
import sisenseCloudLogo from '/src/assets/images/integrations/sisense-cloud.png'
import powerbiLogo from '/src/assets/images/integrations/powerbi.png'
import pythonLogo from '/src/assets/images/integrations/python.png'
import redashLogo from '/src/assets/images/integrations/redash.png'
import rakamLogo from '/src/assets/images/integrations/rakam.svg'
import datagripLogo from '/src/assets/images/integrations/datagrip.png'
import dbeaverLogo from '/src/assets/images/integrations/dbeaver.png'
import modeLogo from '/src/assets/images/integrations/mode.png'
import thoughtSpotLogo from '/src/assets/images/integrations/thoughtspot.jpeg'

export default [{
  label: 'Rakam',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/rakam.md'), loadingComponent: ElSkeleton}),
  logo: rakamLogo,
  category: 'BI Tools',
  publicDocs: 'https://docs.rakam.io/docs'
},{
  label: 'Tableau',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/tableau.vue'), loadingComponent: ElSkeleton}),
  logo: tableauLogo,
  category: 'BI Tools',
  publicDocs: 'https://metriql.com/integrations/bi-tools/tableau',
  beta: true
}, {
  label: 'Google Data Studio',
  logo: dataStudioLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/datastudio.vue'), loadingComponent: ElSkeleton}),
  publicDocs: 'https://metriql.com/integrations/bi-tools/google-data-studio',
  category: 'BI Tools',
},  {
  label: 'Looker',
  logo: lookerLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/looker.vue'), loadingComponent: ElSkeleton}),
  publicDocs: 'https://metriql.com/integrations/bi-tools/looker',
  category: 'BI Tools',
}, {
  label: 'Google Sheets',
  logo: googleSheetsLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/googlesheets.md'), loadingComponent: ElSkeleton}),
  category: 'Others',
  publicDocs: 'https://metriql.com/integrations/services/google-sheets',
  ready: false
}, {
  label: 'Superset',
  category: 'BI Tools',
  logo: supersetLogo,
  publicDocs: 'https://metriql.com/integrations/bi-tools/superset',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/superset.vue'), loadingComponent: ElSkeleton}),
}, {
  label: 'Redash',
  category: 'BI Tools',
  logoStyle: 'max-width:50%',
  logo: redashLogo,
  publicDocs: 'https://metriql.com/integrations/bi-tools/redash',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/redash.vue'), loadingComponent: ElSkeleton}),
}, {
  label: 'Sisense Cloud',
  category: 'BI Tools',
  logoStyle: 'height:30px',
  publicDocs: 'https://metriql.com/integrations/bi-tools/sisense-cloud',
  logo: sisenseCloudLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/sisense-cloud.vue'), loadingComponent: ElSkeleton}),
}, {
  label: 'Metabase',
  category: 'BI Tools',
  logo: metabaseLogo,
  publicDocs: 'https://metriql.com/integrations/bi-tools/metabase',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/metabase.md'), loadingComponent: ElSkeleton}),
  ready: false,
},  {
  label: 'Power BI',
  category: 'BI Tools',
  logo: powerbiLogo,
  ready: false,
}, {
  label: 'Datagrip',
  category: 'SQL Clients',
  logoStyle: 'height:60px',
  logo: datagripLogo,
  publicDocs: 'https://metriql.com/integrations/services/datagrip',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/datagrip.vue'), loadingComponent: ElSkeleton}),
}, {
  label: 'ThoughtSpot',
  category: 'BI Tools',
  logo: thoughtSpotLogo,
  publicDocs: 'https://metriql.com/integrations/bi-tools/thoughtspot',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/thoughtspot.vue'), loadingComponent: ElSkeleton}),
},{
  label: 'Mode Analytics',
  category: 'BI Tools',
  logoStyle: 'height:80px',
  publicDocs: 'https://metriql.com/integrations/bi-tools/mode',
  logo: modeLogo,
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/mode.vue'), loadingComponent: ElSkeleton}),
},{
  label: 'DBeaver',
  category: 'SQL Clients',
  logoStyle: 'height:50px;margin-top:-3px',
  logo: dbeaverLogo,
  publicDocs: 'https://metriql.com/integrations/services/dbeaver',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/dbeaver.vue'), loadingComponent: ElSkeleton}),
}, {
  label: 'Python',
  category: 'Developer Tools',
  logo: pythonLogo,
  publicDocs: 'https://metriql.com/integrations/services/python',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/python.vue'), loadingComponent: ElSkeleton}),
}, {
  label: 'REST API',
  category: 'Developer Tools',
  publicDocs: 'https://metriql.com/integrations/rest-api',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/rest.vue'), loadingComponent: ElSkeleton}),
  full: true
}, {
  label: 'CLI',
  publicDocs: 'https://metriql.com/integrations/services/cli',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/cli.vue'), loadingComponent: ElSkeleton}),
  category: 'Developer Tools'
}, {
  label: 'JDBC Driver',
  publicDocs: 'https://metriql.com/integrations/jdbc-driver',
  docs: defineAsyncComponent({loader: () => import('/src/assets/docs/jdbc.vue'), loadingComponent: ElSkeleton}),
  category: 'BI Tools'
}].sort((a, b) => {
  return a.ready === false ? (a.label.localeCompare(b.label)) : (a.label.localeCompare(b.label))
})
