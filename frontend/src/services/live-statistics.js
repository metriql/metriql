import { MetriqlAdmin } from './MetriqlAdmin'

class LiveStatistics {
  points = []
  hooks = []

  constructor () {
    setInterval(() => {
      MetriqlAdmin.countOfActiveTasks().then(count => {
        let row = [new Date(), count]
        this.points.push(row)
        this.hooks.forEach(hook => {
          hook(row, this.points)
        })
      })
    }, 5000)
  }

  register(func) {
    this.hooks.push(func)
  }
}

export default new LiveStatistics()
