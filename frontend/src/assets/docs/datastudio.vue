<template>
  <ol>
    <li><a href="https://datastudio.google.com/datasources/create?connectorId=AKfycbw8o0F6LEr0epNSNVWqNzlqo7R-6jRYxxSxBspzyg2Xi6SDFItLN_aM3l_U56Z0obwS" target="_blank">Install metriql Connector</a> to your Data Studio account.</li>
    <li>
      <template v-if="!$BASE.IS_LOCAL">
      Enter URL:
      <el-tooltip  content="Click to copy" placement="top">
        <button  @click="$clipboard($BASE.URL)" el-tooltip="Click to copy">{{$BASE.URL}}</button>
      </el-tooltip>
      </template>
      <div v-else>
        Your server is in your local environment (<b>{{$BASE.URL}}</b>) so you need to expose your local port to the internet so that Google servers can access it. <br>
        <a href="https://github.com/anderspitman/awesome-tunneling" target="blank">Here</a> is a few tunneling resources that let you expose your local port.
      </div>
    </li>

    <li>
      <template v-if="$BASE.IS_LOCAL">
        Once you expose your port, enter the URL,
      </template>
      Select your dataset and click `Create Report`.</li>
    <li>You're done. 🎉</li>
  </ol>
</template>
