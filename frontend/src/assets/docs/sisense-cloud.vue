<template>
  <div>
    <div v-if="$BASE.IS_LOCAL">
      Your server is in your local environment (<b>{{$BASE.URL}}</b>) so you need to expose your local port to the internet so that Sisense servers can access it.
      <a href="https://github.com/anderspitman/awesome-tunneling" target="blank">Here</a> is a few tunneling resources that let you expose your local port.
    </div>

    <table class="el-table">
      <tr>
        <th>Config</th>
        <th>Value</th>
      </tr>
      <tr>
        <td>Database Host</td>
        <td>{{$BASE.HOSTNAME}}</td>
      </tr>
      <tr>
        <td>Database Port</td>
        <td>{{$BASE.PORT}}</td>
      </tr>
      <tr>
        <td>Database name</td>
        <td>metriql</td>
      </tr>
      <tr>
        <td>Database Username</td>
        <td><i>YOUR_METRIQL_USERNAME</i></td>
      </tr>
      <tr>
        <td>Database Password</td>
        <td><i v-if="!isPasswordless">YOUR_METRIQL_PASSWORD</i><i v-else>(empty)</i></td>
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
