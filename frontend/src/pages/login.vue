<template>
  <div style="padding: 10px">
    <el-form ref="formElem" :model="form" label-position="left" label-width="auto" style="margin: 100px auto; width: 350px">
      <el-form-item :rules="[{required: true}]" label="Username" prop="username">
        <el-input v-model="form.username"></el-input>
      </el-form-item>
      <el-form-item :rules="[{required: true}]"  label="Password" prop="password">
        <el-input type="password" v-model="form.password"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="login">Login</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>

import { ref } from 'vue'
import { AuthService } from '/src/services/auth'
import { MetriqlAdmin } from '../services/MetriqlAdmin'
import { ElMessage } from 'element-plus'
import router from '../router'


export default {
  setup () {
    const formElem = ref(null)
    const form = ref({username: '', password: ''})
    const login = function () {
      formElem.value.validate(valid => {
        if(valid) {
          const headerValue = AuthService.getBasicAuthHeader(form.value.username, form.value.password)
          MetriqlAdmin.getMetadata(headerValue).then(response => {
            AuthService.setAuth(headerValue)
            router.push('/')
          }).catch(error => {
            debugger
            ElMessage({
              message: 'Invalid username & password',
              showClose: true,
              type: 'error'
            })
          })
        }
      })
    }

    return {form, formElem, login}
  }
}
</script>
