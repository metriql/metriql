<template>
  <ol>
    <li> <el-input v-model="connection" placeholder="enter connection name in your Looker instance (default is metriql)"/> </li>
    <li style="margin-top:10px"> <el-link :href="`${$BASE_URL}/api/v0/integration/looker?connection=${connection || 'metriql'}`" target="_blank">Download LookML project</el-link> </li>
  </ol>
</template>
<script>

export default {
  data() {
    return {
      connection: ''
    }
  }
}
</script>
