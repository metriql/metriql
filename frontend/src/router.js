import { createRouter, createWebHistory } from 'vue-router'
import Integrations from '/src/pages/integrations.vue'
// import Playground from '/src/pages/playground.vue'
import Monitoring from '/src/pages/monitoring.vue'
import Login from '/src/pages/login.vue'
import NotFound from '/src/components/NotFound.vue'

const base = import.meta.env.BASE_URL
const routes = [
  {path: base + '/login', name: 'Login', component: Login},
  {path:  base + '/', name: 'Integrations', component: Integrations},
  // {path: base+'/playground', name: 'Playground', component: Playground},
  {path:  base + '/monitoring', name: 'Monitoring', component: Monitoring},
  { path:  base + "/:catchAll(.*)", name: "404", component: NotFound }
]

export default createRouter({
  history: createWebHistory(),
  routes,
})
