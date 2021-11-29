<template>
  <ol>
    <li>
      <dataset-selector v-model="dataset" />
    </li>
    <li style="margin-top:5px">
      <el-button @click="download" plain :disabled="dataset == null">Download TDS file</el-button>
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
import { AuthService, AuthType } from '/src/services/auth'
import DatasetSelector from '/src/components/DatasetSelector.vue'
import { download } from '../../services/request'

export default {
  components: {DatasetSelector},
  props: {
    value: Object
  },
  data: function() {
    return {
      dataset: null
    }
  },
  methods: {
    download() {
      download(`/api/v0/integration/tableau?dataset=${this.dataset}`);
    }
  },
  computed: {
    isPasswordless: function() {
      return AuthService.getCurrentAuthType() == AuthType.NONE
    }
  }
}
</script>
