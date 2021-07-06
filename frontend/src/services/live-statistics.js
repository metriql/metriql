import axios from 'axios'
import { AuthService } from './auth'
import { BASE_URL } from './request'

class LiveStatistics {
  points = []
  hooks = []
  errorHooks = []
  deactivatedError = null

  constructor () {
    setInterval(() => {
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
        if (this.deactivatedError) return
        this.deactivatedError = e
        this.errorHooks.forEach(hook => {
          hook(e)
        })
      })
    }, 5000)
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
