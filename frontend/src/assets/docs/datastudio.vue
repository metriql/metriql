<template>
  <ol>
    <li><a href="https://datastudio.google.com/datasources/create?connectorId=AKfycbw8o0F6LEr0epNSNVWqNzlqo7R-6jRYxxSxBspzyg2Xi6SDFItLN_aM3l_U56Z0obwS" target="_blank">Install metriql Connector</a> to your Data Studio account.</li>
    <li>
      Enter URL:
      <el-tooltip content="Click to copy" placement="top">
        <button @click="$clipboard($BASE_URL)" el-tooltip="Click to copy">
        {{$BASE_URL}}
        </button>
      </el-tooltip>
    </li>

    <li>Select your dataset and click `Create Report`.</li>
    <li>You're done. 🎉</li>
  </ol>
</template>
