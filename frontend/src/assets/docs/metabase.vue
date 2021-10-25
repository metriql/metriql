<template>
  <div>
    <el-form ref="form" :model="credentials" label-position="left" label-width="120px">
      <el-form-item label="Metabase URL" prop="metabase_url" :rules="[{ required: true }]">
        <el-input type="url" v-model="credentials.metabase_url"></el-input>
      </el-form-item>
      <el-form-item label="Email address" prop="metabase_username" :rules="[{ required: true }]">
        <el-input type="text" v-model="credentials.metabase_username" required></el-input>
      </el-form-item>
      <el-form-item label="Password" prop="metabase_password" :rules="[{ required: true }]">
        <el-input type="password" v-model="credentials.metabase_password" required></el-input>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="fetchDatabases" :loading="databases.loading">Fetch databases</el-button>
      </el-form-item>
      <el-form-item v-if="databases.list != null">
        <el-select v-if="databases.list.length > 0" v-model="credentials.database" placeholder="Select">
          <el-option
            v-for="item in databases.list"
            :key="item.id"
            :label="item.name"
            :value="item.id">
          </el-option>
        </el-select>
        <el-alert v-else>
          You don't have any Trino dataset, please create a database as follows.
        </el-alert>

        <div>
          Here is the configuration that you need if you want to create a new database:

          <table class="el-table">
            <tr>
              <th>Config</th>
              <th>Value</th>
            </tr>
            <tr>
              <td>Database Type</td>
              <td>Presto / Trino</td>
            </tr>
            <tr>
              <td>Host</td>
              <td>{{$BASE.HOSTNAME}}</td>
            </tr>
            <tr>
              <td>Port</td>
              <td>{{$BASE.PORT}}</td>
            </tr>
            <tr>
              <td>Database name</td>
              <td>metriql</td>
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
              <td>Use a secure connection (SSL)?</td>
              <td>{{$BASE.PROTOCOL == 'https:'}}</td>
            </tr>
          </table>
          <div v-if="databases.list.length === 0">
            Once you create the database, you can `Fetch databases` again and select the database that you created for
            Metriql.
          </div>
        </div>


      </el-form-item>
      <el-form-item v-if="credentials.database">
        <el-button type="primary" @click="sync" :loading="syncing">Sync datasets</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
import { request } from '../../services/request'

export default {
  data () {
    return {
      syncing: false,
      credentials: {
        metabase_url: null,
        metabase_username: null,
        metabase_password: null,
        database: null
      },
      databases: {
        loading: false,
        list: null
      }
    }
  },
  computed: {
    url: function () {
      return `trino://USERNAME:PASSWORD@${this.$BASE.HOSTNAME}:${this.$BASE.PORT}/metriql?protocol=${this.$BASE.PROTOCOL.replace(':', '')}`
    }
  },
  methods: {
    fetchDatabases () {
      this.$refs.form.validate((valid) => {
        if (!valid) {
          return
        }

        this.databases.loading = true
        request.post('/api/v0/integration/metabase', {
          action: 'list-databases',
          parameters: this.credentials
        }).then(response => {
          this.databases.list = response.data
        }).finally(() => {
          this.databases.loading = false
        })
      })
    },
    sync () {
      this.syncing = true
      request.post('/api/v0/integration/metabase', {action: 'sync-database', parameters: this.credentials})
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
