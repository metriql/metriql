import axios from 'axios'
import { AuthService } from './auth'
import { BASE_URL } from './request'
import router from "../router"

class LiveStatistics {
  points = []
  hooks = []
  errorHooks = []
  deactivatedError = null

  constructor () {
    let handler = () => {
      let currentRoute = router.currentRoute
      if(currentRoute.value.name === 'Login') return

      axios.get(`${BASE_URL}/api/v0/task/activeCount`,
        {
          headers: {
            'Authorization': AuthService.getAuth(),
            'content-type': 'application/json'
          }
        }).then(response => {
        let row = [new Date(), response.data]
        this.points.push(row)
        this.hooks.forEach(hook => {
          hook(row, this.points)
        })
      }).catch(e => {
        if(e.response.status === 401) {
          router.push('/ui/login')
        }

        if (this.deactivatedError) return
        this.deactivatedError = e
        this.errorHooks.forEach(hook => {
          hook(e)
        })
      })
    }
    setInterval(handler, 5000)
    setTimeout(handler, 1)
  }

  retry () {
    this.deactivatedError = null
  }

  register (func, errorFunc) {
    this.hooks.push(func)
    if(errorFunc != null) {
      this.errorHooks.push(errorFunc)
    }
  }
}

export default new LiveStatistics()
