<template>
  <ol>
    <li>
      <a href="https://trino.io/download.html" target="_blank">Download</a> the JDBC driver from Trino.
    </li>
    <li v-if="$BASE.PROTOCOL === 'http:'">
      Your server requires username & password so SSL is required. If you're running metriql locally and SSL is not enabled in your environment (<b>{{$BASE.URL}}</b>), you can use
      <a href="https://github.com/anderspitman/awesome-tunneling" target="blank">tunneling tools</a> to issue temporary SSL certificate and use the tunnel URL.
    </li>
    <li>
      Use following JDBC URL: <br>
      <el-tooltip content="Click to copy" placement="top">
      <button @click="$clipboard(url) && $event.preventDefault()">
        {{url}}
      </button>
      </el-tooltip>
    </li>
    <li>
      <a href="https://metriql.com/integrations/jdbc-driver" target="_blank">See the documentation</a>
      to learn more about how you can run SQL queries via the driver.
    </li>
  </ol>
</template>
<script>
import { AuthService, AuthType } from '/src/services/auth'

export default {
  computed: {
    url: function() {
      const isPasswordless = AuthService.getAuth() == AuthType.NONE
      return `jdbc:trino://${this.$BASE.HOST}?username=USERNAME` + (!isPasswordless ? '&password=PASSWORD' : '') + ((this.$BASE.PROTOCOL === 'https:' || !isPasswordless)  ? '&SSL=true' : '')
    }
  }
}
</script>
