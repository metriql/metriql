<template>
  <app-subnav :sections="sections">

  </app-subnav>
  <main>
    <div ref="editor" class="editor" />

    <el-progress :percentage="100" :format="() => 'Running'" :indeterminate="true" style="width:30%;margin:5px 0" />

  </main>
</template>
<script>
import AppSubnav from '/src/components/AppSubnav.vue'
import * as monaco from 'monaco-editor'
import EditorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker'
import YamlWorker from 'monaco-yaml/lib/esm/yaml.worker?worker'
import { onMounted, onUnmounted, ref } from 'vue'

self.MonacoEnvironment = {
  getWorker (name, label) {
    if (['yml', 'yaml'].includes(label)) {
      return new YamlWorker()
    }
    return new EditorWorker()
  }
}

const sections = [{
  id: 'segmentation',
  label: 'Segmentation',
  pageId: 'playground'
}, {
  id: 'funnel',
  label: 'Funnel',
  pageId: 'playground'
}, {
  id: 'retention',
  label: 'Retention',
  pageId: 'playground'
}, {
  id: 'sql',
  label: 'SQL',
  pageId: 'playground'
}, {
  id: 'mql',
  label: 'MQL',
  pageId: 'playground'
}]

export default {
  name: 'Playground',
  components: {AppSubnav},
  setup () {
    const editor = ref(null)

    let monacoEditor
    onMounted(() => {
      monacoEditor = monaco.editor.create(editor.value, {
        language: 'yml',
        value: `dimensions`
      })
    })

    onUnmounted(() => {
      monacoEditor.dispose()
    })

    return {sections, editor}
  }
}
</script>
<style lang="scss">
.editor {
  width: 100%;
  height: 300px;
  border: 1px solid #eee
}
</style>
