import { createRouter, createWebHistory } from 'vue-router'
import Integrations from '/src/pages/integrations.vue'
// import Playground from '/src/pages/playground.vue'
import Monitoring from '/src/pages/monitoring.vue'
import Login from '/src/pages/login.vue'
import NotFound from '/src/components/NotFound.vue'

const routes = [
  {path: '/login', name: 'Login', component: Login},
  {path: '/', name: 'Integrations', component: Integrations},
  // {path: '/playground', name: 'Playground', component: Playground},
  {path: '/monitoring', name: 'Monitoring', component: Monitoring},
  { path: "/:catchAll(.*)", name: "404", component: NotFound }
]

export default createRouter({
  history: createWebHistory(),
  routes,
})
