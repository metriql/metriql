<template>
  <ol>
    <li>
      <a href="https://trino.io/docs/current/installation/jdbc.html" target="_blank">Download</a> the JDBC driver from Trino.
    </li>
    <li>
      Use following JDBC URL: <br>
      <el-tooltip content="Click to copy" placement="top">
      <button @click="$clipboard(url)">
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
      return `jdbc:trino://${this.$BASE.HOST}?username=USERNAME` + (isPasswordless ? '&password=PASSWORD' : '')
    }
  }
}
</script>
