
export class AuthService {
  static getBasicAuthHeader(username, password) {
    return `Basic ${btoa(`${username}:${password}`)}`
  }

  static setAuth(value) {
    localStorage.setItem('metriql-auth', value);
  }

  static getAuth() {
    return localStorage.getItem('metriql-auth');
  }

  static logout() {
    localStorage.removeItem('metriql-auth');
  }
}
