<template>
  <div>
    <p><b>Step 1</b>: Use `Starburst` database to connect Metriql and use the following credentials:</p>

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
        <td>User</td>
        <td><i>YOUR_METRIQL_USERNAME</i></td>
      </tr>
      <tr>
        <td>Pass</td>
        <td><i v-if="!isPasswordless">YOUR_METRIQL_PASSWORD</i><i v-else>(empty)</i></td>
      </tr>
      <tr>
        <td>Database</td>
        <td>metriql</td>
      </tr>
    </table>

    <p><b>Step 2</b>: Select all datasets and press `Create connection`.</p>

    <p><b>Step 3</b>: Thoughtspot has TML to model the data but we don't support generating TML files yet. While you can query Metriql data from Thoughtspot, the formatting & measure mapping should be done manually for now. Please refer to the documentation to learn more about the current status of the integration.</p>
  </div>
</template>
