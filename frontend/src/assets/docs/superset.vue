<template>
  <div>
  <el-form ref="form" :model="credentials" label-position="left" label-width="120px">
    <el-form-item label="Superset URL" prop="superset_url" :rules="[{ required: true }]">
      <el-input type="url" v-model="credentials.superset_url" ></el-input>
    </el-form-item>
    <el-form-item label="Username" prop="superset_username" :rules="[{ required: true }]">
      <el-input type="text" v-model="credentials.superset_username" required></el-input>
    </el-form-item>
    <el-form-item label="Password" prop="superset_password" :rules="[{ required: true }]">
      <el-input type="password" v-model="credentials.superset_password" required></el-input>
    </el-form-item>
    <el-form-item>
      <el-button type="primary" @click="fetchDatabases" :loading="databases.loading">Fetch databases</el-button>
    </el-form-item>
    <el-form-item v-if="databases.list != null">
      <el-select v-model="credentials.database_id" placeholder="Select">
        <el-option
          v-for="item in databases.list"
          :key="item.id"
          :label="item.name"
          :value="item.id">
        </el-option>
      </el-select>

      <div>
      You can use the following URL to create a new database for metriql:

      <button>
        trino://USERNAME:PASSWORD@{{$BASE.HOSTNAME}}:{{ $BASE.PORT }}/metriql?protocol={{$BASE.PROTOCOL.replace(':', '')}}
      </button>
      <div v-if="$BASE.PROTOCOL === 'https:'">
        Since you enabled SSL, the `engine_params` should be as follows:
        <pre>{"engine_params": {"connect_args": {"http_scheme": "https"}}}</pre>
      </div>
        <div v-if="databases.list.length === 0">
          Once you create the database, you can `Fetch databases` again and select the database that you created for Metriql.
        </div>
      </div>


    </el-form-item>
    <el-form-item v-if="credentials.database_id">
      <el-button type="primary" @click="sync" :loading="databases.loading">Sync datasets</el-button>
    </el-form-item>
  </el-form>
  </div>
</template>
<script>
import { download, request } from '../../services/request'

export default {
  data() {
    return {
      syncing: false,
      credentials: {
        superset_url: null,
        superset_username: null,
        superset_password: null,
        database_id: null
      },
      databases: {
        loading: false,
        list: null
      }
    }
  },
  methods: {
    fetchDatabases() {
      this.$refs.form.validate((valid) => {
        if (!valid) {
          return;
        }

        this.databases.loading = true
        request.post('/api/v0/integration/superset', {action: 'list-databases', parameters: this.credentials}).then(response => {
          this.databases.list = response.data
        }).finally(() => {
          this.databases.loading = false
        })
      });
    },
    sync() {
      this.syncing = true
      request.post('/api/v0/integration/superset', {action: 'sync-database', parameters: this.credentials})
        .then(response => {
          this.$message({
            type: 'success',
            message: response.data
          })
        })
        .finally(() => {
        this.syncing = false
      })
    }
  }
}
</script>
