<template>
  <div>
    <div v-if="$BASE.IS_LOCAL">
      Your server is in your local environment (<b>{{$BASE.URL}}</b>) so you need to expose your local port to the internet so that Mode servers can access it.
      <a href="https://github.com/anderspitman/awesome-tunneling" target="blank">Here</a> is a few tunneling resources that let you expose your local port.
    </div>

    <table class="el-table">
      <tr>
        <th>Config</th>
        <th>Value</th>
      </tr>
      <tr>
        <td>Host and Port</td>
        <td>Host: <b>{{$BASE.HOSTNAME}}</b> Port: <b>{{$BASE.PORT}}</b></td>
      </tr>
      <tr>
        <td>Driver</td>
        <td>Latest Trino version</td>
      </tr>
      <tr>
        <td>LDAP</td>
        <td v-if="isPasswordless">Username: mode</td>
        <td v-else="isPasswordless">Username: YOUR_METRIQL_USERNAME Password: YOUR_METRIQL_PASSWORD</td>
      </tr>
    </table>
  </div>
</template>
<script>
import { AuthService, AuthType } from '../../services/auth'

export default {
  data() {
    return {
    }
  },
  computed: {
    isPasswordless: function() {
      return AuthService.getAuth() == AuthType.NONE
    }
  }
}
</script>
