<template>
  <header v-if="route.name != 'Login'">
    <app-navbar />
  </header>
  <router-view/>
</template>

<script>
import AppNavbar from './components/AppNavbar.vue';
import { useRoute } from 'vue-router';

export default {
  name: 'App',
  components: {
    AppNavbar
  },
  setup() {
    const route = useRoute();
    return {route}
  }
}
</script>

<style lang="scss">
#rkm-app {
  background-color: var(--rkm-app-background-color);
  height: 100vh;

  main {
    //display: flex;
    //flex-flow: row wrap;
    padding: 38px 49px;
  }

  .logo {
    width: 64px;
  }
}
</style>
