<template>
  <div>
    If your Redash version is <b>>= v10.0.0</b>, you can use Trino data source to connect Metriql. Otherwise, you should use Presto data source.
    Here is the information about the data source for your Metriql deployment:
    <table class="el-table">
      <tr>
        <th>Config</th>
        <th>Value</th>
      </tr>
      <tr>
        <td>Host and Port</td>
        <td>{{$BASE.HOSTNAME}}</td>
      </tr>
      <tr>
        <td>Protocol</td>
        <td>{{$BASE.PROTOCOL}}</td>
      </tr>
      <tr>
        <td>Port</td>
        <td>{{$BASE.PORT}}</td>
      </tr>
      <tr>
        <td>Username</td>
        <td><i>YOUR_METRIQL_USERNAME</i></td>
      </tr>
      <tr>
        <td>Password</td>
        <td><i v-if="!isPasswordless">YOUR_METRIQL_PASSWORD</i><i v-else>(empty)</i></td>
      </tr>
      <tr>
        <td>Schema</td>
        <td>public</td>
      </tr>
      <tr>
        <td>Catalog</td>
        <td>metriql</td>
      </tr>
    </table>
    <br>
    Redash doesn't have metrics so you can run SQL queries referencing your dimensions & measures in your datasets. Please see <a href="https://metriql.com/query/mql" target="_blank">MQL reporting type</a> for documentation.
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
