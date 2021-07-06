<template>
  <ol>
    <li>
      <dataset-selector v-model="dataset" />
    </li>
    <li style="margin-top:5px">
      <el-link :href="`${$BASE_URL}/api/v0/integration/tableau?dataset=${dataset}`" :disabled="dataset == null" target="_blank">Download TDS file</el-link>
    </li>
    <li>
      Open TDS file with Tableau.
    </li>
    <li v-if="isPasswordless">
      Enter your username and leave password blank. (Entering a password prevents Tableau to connect metriql)
    </li>
    <li v-else>
      Enter your username & password.
    </li>
  </ol>
</template>
<script>
import { ref } from 'vue'
import { AuthService, AuthType } from '/src/services/auth'
import DatasetSelector from '/src/components/DatasetSelector.vue'

export default {
  components: {DatasetSelector},
  props: {
    value: Object
  },
  setup (props) {
    const isPasswordless = AuthService.getAuth() == AuthType.NONE
    const dataset = ref(null)
    return {dataset, isPasswordless}
  }
}
</script>
