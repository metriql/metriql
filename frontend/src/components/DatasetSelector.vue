<template>
  <el-cascader :modelValue="modelValue" @update:modelValue="$emit('update:modelValue', $event[0])" :options="data"
               placeholder="Select dataset"/>
</template>
<script>
import { MetriqlAdmin } from '../services/MetriqlAdmin'
import { onMounted, ref } from 'vue'

export default {
  props: {
    modelValue: String,
    modelModifiers: {
      default: () => ({})
    }
  },
  emits: ['update:modelValue'],
  setup () {
    const data = ref(null)

    onMounted(async () => {
      const datasets = await MetriqlAdmin.getMetadata()

      const values = new Set()
      datasets.map(dataset => {
        if (dataset.category) {
          values.add(dataset.category)
        }
      })

      if (values.length > 0) {
        const rows = []
        values.forEach(value => {
          rows.push({
            value: value,
            label: value,
            children: datasets
              .filter(dataset => dataset.category == value)
              .map(dataset => {
                return {
                  value: dataset.name,
                  label: dataset.label || dataset.name
                }
              })
          })
        })
        data.value = rows
      } else {
        data.value = datasets
          .map(dataset => {
            return {
              value: dataset.name,
              label: dataset.label || dataset.name
            }
          })
      }
    })

    return {data}
  }

}
</script>
