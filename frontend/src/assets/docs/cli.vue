<template>
  <ol>
    <li>
      metriql is compatible with Trino, you can download the trino CLI and use metriql URL <a href="https://trino.io/download.html">from here</a>.
    </li>
    <li>
      Execute the following command if you're running metriql using the default port: <br>
      <el-tooltip content="Click to copy" placement="top">
        <button @click="$clipboard(command)">
          {{command}}
        </button>
      </el-tooltip>
    </li>
    <li>
      <a href="https://metriql.com/integrations/jdbc-driver">Follow the documentation</a> in order to learn how to run different type of queries.
    </li>
  </ol>
</template>
<script>
import { AuthService, AuthType } from '/src/services/auth'

export default {
  computed: {
    command: function() {
      const isPasswordless = AuthService.getAuth() == AuthType.NONE
      return `trino-cli --host ${this.$BASE.HOSTNAME} --port ${this.$BASE.PORT}` + (isPasswordless ? '--password' : '')
    }
  }
}
</script>

