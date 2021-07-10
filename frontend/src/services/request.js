import axios from 'axios'
import { ElMessage, ElMessageBox } from 'element-plus'
import { AuthService } from '/src/services/auth'
import router from '../router'

let protocol = import.meta.env.VITE_BACKEND_PROTOCOL
let host = import.meta.env.VITE_BACKEND_HOST
export const BASE_URL = host ? ((protocol || 'http:') + '//' + host) : ''

export const xhrOptions = {
  baseURL: BASE_URL,
  headers: {'content-type': 'application/json'}
}

export const request = axios.create(xhrOptions)

request.interceptors.request.use(request => {
  const auth = AuthService.getAuth()
  let commonHeaders = request.headers.common

  if (auth != null && commonHeaders['Authorization'] == null) {
    commonHeaders['Authorization'] = auth
  }
  if(BASE_URL != null) {
    // commonHeaders['Origin'] = BASE_URL
  }
  return request
}, error => Promise.reject(error))

request.interceptors.response.use(response => response, function (error) {
  let responseExist = error.response
  let status = responseExist ? error.response.status : 0
  let messageBox = {
    confirmButtonText: 'OK',
    showCancelButton: false,
    showClose: false,
    closeOnClickModal: false,
    closeOnPressEscape: false,
    type: 'error'
  }
  let reloadPage = () => {
    location.reload()
  }

  if (status == 0) {
    ElMessageBox.confirm('Unable to reach the server, please check your network.', 'Network issue', messageBox).then(reloadPage)
  } else if (status == 401) {
    const auth = AuthService.getAuth()
    if (auth != null) {
      ElMessageBox.confirm('Unable to authenticate you, refresh the page.', 'Auth issue', messageBox).then(() => {
        AuthService.logout()
        router.push('/login')
      })
    } else {
      router.push({path: '/login'})
    }
  } else if (status >= 400 && status <= 504) {
    let message
    if (status !== 404) {
      const p = document.createElement('p')
      if (error.response.data && error.response.data.errors) {
        error.response.data.errors.map(error => {
          const span = document.createElement('span')
          span.innerText = error.title
          return span
        }).forEach(span => {
          p.appendChild(span)
        })
      } else {
        p.innerText = 'An error occurred while connecting the server, please reach our the Administrator'
      }

      message = p.outerHTML
    } else {
      message = 'The resource does not exist or you don\'t have the permission'
    }

    ElMessage({
      message: message,
      dangerouslyUseHTMLString: true,
      showClose: true,
      type: 'error'
    })
  } else {
    console.error(`unknown status ${status}`, error)
  }

  // Do something with response error
  return Promise.reject(error)
})
