
export const AuthType = Object.freeze({
  NONE:   Symbol("NONE"),
  BASIC:  Symbol("BASIC"),
});

export class AuthService {
  static getBasicAuthHeader(username, password) {
    return `Basic ${btoa(`${username}:${password}`)}`
  }

  static getCurrentAuthType() {
    if(AuthService.getAuth() != null) {
      return AuthType.BASIC
    } else {
      return AuthType.NONE
    }
  }

  static setAuth(value) {
    debugger
    localStorage.setItem('metriql-auth', value);
  }

  static getAuth() {
    return localStorage.getItem('metriql-auth');
  }

  static logout() {
    debugger
    localStorage.removeItem('metriql-auth');
  }
}
