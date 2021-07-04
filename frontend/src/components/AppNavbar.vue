<template>
  <nav class="rkm-c-navbar">
    <div class="rkm-c-navbar__logo">
      <slot name="logo">
        <img src="../assets/logo.svg" class="logo">
      </slot>
    </div>
    <ul role="menubar" class="rkm-c-navbar__menu">
      <li role="none" class="rkm-c-navbar__item" :class="{ 'is-active': ('Integrations' === $route.name) }">
        <router-link role="menuitem" :to="baseUrl" class="rkm-c-navbar__link">Integrations</router-link>
      </li>
      <li role="none" class="rkm-c-navbar__item" :class="{ 'is-active': ('Monitoring' === $route.name) }">
        <el-badge :value="activeTaskCount" :hidden="!(activeTaskCount > 0)" type="info">
          <router-link role="menuitem" :to="`${baseUrl}monitoring`" class="rkm-c-navbar__link">Live Queries</router-link>
        </el-badge>
      </li>
    </ul>
    <div class="rkm-c-navbar__cta">
      <el-button type="primary" class="sync-button" @click="sync" :loading="syncing">
        <i class="el-icon-refresh" />
        Sync</el-button>
    </div>
  </nav>
</template>

<script>
import {MetriqlAdmin} from "/src/services/MetriqlAdmin";
import LiveStatistics from "/src/services/live-statistics";
import { ref } from 'vue'

export default {
  name: 'AppNavbar',
  methods: {
    sync: function() {
      this.syncing = true
      MetriqlAdmin.updateManifest()
        .then(() => {
          this.$message({
            type: 'success',
            message: 'Synchronized manifest file successfully!'
          })
        })
        .finally(() => {
        this.syncing = false
      })
    }
  },
  setup() {
    const syncing = ref(false)
    const activeTaskCount = ref(null)

    LiveStatistics.register(function(count) {
      activeTaskCount.value = count[1]
    })

    const baseUrl = ref(import.meta.env.BASE_URL)

    return {syncing, baseUrl, activeTaskCount}
  }
}
</script>

<style lang="scss" scoped>
.sync-button {
  background: linear-gradient(33deg, rgb(154 91 215) 14%, rgb(135,12,254) 100%);
  border-color: rgb(154 89 217);
  box-shadow: -1px 2px 2px #000;

  &:hover {
    background: linear-gradient(33deg, rgb(154 91 215) 14%, rgb(135,12,254) 100%);
    border-color: rgb(154 89 217);
  }

  &:focus, &:active {
    background: rgb(154 91 215);
    border-color: rgb(154 89 217);
    box-shadow: 0px 1px 3px #000;

  }
}
.rkm-c-navbar {
  display: flex;
  align-items: center;
  background-color: var(--rkm-navbar-background-color);
  padding: 18px 32px;
  box-shadow: 0 2px 4px 0 rgba(0, 0, 0, 0.2);

  &__logo {
    flex-basis: 20%;
    display: flex;
  }

  &__menu {
    flex-basis: 60%;
    display: flex;
    flex-flow: row wrap;
    justify-content: center;
    list-style-type: none;
    margin: 0;
    padding: 0;
  }

  &__item {
    padding: 18px 32px;
    color: var(--rkm-navbar-link-color);

    &.is-active {
      background-color: var(--rkm-navbar-active-background-color);
      color: var(--rkm-navbar-active-color);
      border-radius: var(--rkm-border-radius-base);
    }
  }

  &__link {
    color: inherit;
    font-size: var(--rkm-base);
    font-weight: var(--rkm-font-weight-medium);

    &:hover {
      color: var(--rkm-navbar-hover-color);
    }
  }

  &__cta {
    flex-basis: 20%;
    display: flex;
    justify-content: flex-end;
  }
}
</style>
