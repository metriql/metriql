import {request} from '/src/services/request'
import { AuthService } from './auth'

export class MetriqlAdmin {
  static updateManifest() {
    return request.put('/api/v0/update-manifest').then(response => response.data)
  }

  static getMetadata(customAuth) {
    return request.get('/api/v0/metadata', {headers: {"Authorization": customAuth || AuthService.getAuth()}}).then(response => response.data)
  }

  static getTasks(data) {
    return request.get('/api/v0/task/list', data).then(response => response.data)
  }

  static countOfActiveTasks() {
    return request.get('/api/v0/task/activeCount').then(response => response.data)
  }
}
