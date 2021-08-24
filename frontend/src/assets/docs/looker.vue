<template>
  <ol>
    <li> <el-input v-model="connection" placeholder="enter connection name in your Looker instance (default is metriql)"/> </li>
    <li style="margin-top:10px"> <el-button @click="download">Download LookML project</el-button> </li>
  </ol>
</template>
<script>
import { download } from '../../services/request'

export default {
  data() {
    return {
      connection: ''
    }
  },
  methods: {
    download() {
      download(`/api/v0/integration/looker?connection=${this.connection || 'metriql'}`);
    }
  }
}
</script>
