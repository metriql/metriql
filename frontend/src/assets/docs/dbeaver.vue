<template>
  <div>
    You should use <b>Trino</b> or <b>Presto</b> driver to connect Metriql. Here is the configuration:

    <table class="el-table">
      <tr>
        <th>Config</th>
        <th>Value</th>
      </tr>
      <tr>
        <td>Host</td>
        <td>{{ $BASE.HOSTNAME }}</td>
      </tr>
      <tr>
        <td>Port</td>
        <td>{{ $BASE.PORT }}</td>
      </tr>
      <tr>
        <td>Database/Schema</td>
        <td>metriql</td>
      </tr>
      <tr>
        <td>Database Username</td>
        <td><i>YOUR_METRIQL_USERNAME</i></td>
      </tr>
      <tr>
        <td>Database Password</td>
        <td><i v-if="!isPasswordless">YOUR_METRIQL_PASSWORD</i><i v-else>(leave it empty)</i></td>
      </tr>
    </table>

    <p v-if="$BASE.IS_LOCAL">
      Your server is in your local environment (<b>{{ $BASE.URL }}</b>) so you should not enable SSL.
    </p>
  </div>
</template>
