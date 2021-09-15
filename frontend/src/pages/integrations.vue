<template>
  <main>
    <div v-if="!activeIntegration" style="width: 100%; max-width:1200px; margin: 0 auto">
      <div class="columns is-gapless">
        <div class="column">
          <span class="el-link" :class="activeCategory == category ? 'el-link--primary' : 'el-link--default'"
                v-for="category in categories" :key="category" style="margin-right:20px"
                @click="activeCategory = category">
            {{ category || 'Show All' }}
          </span>
        </div>
        <div class="column is-narrow">
          <el-input type="search" prefix-icon="el-icon-search" placeholder="type to search" v-model="searchTerm"></el-input>
        </div>
      </div>
      <div v-for="(integration, index) in integrations" :key="index" class="integration-box" :class="{'not-ready': integration.ready == false}">
        <router-link :to="integration.ready != false ? `?integrate=${integration.label}` : '#'">
            <img v-if="integration.logo != null" :src="integration.logo" :style="integration.logoStyle" class="integration-logo"/>
            <span v-else class="integration-logo">{{ integration.label }}</span>
            <div v-if="integration.ready == false" class="integration-tag yellow">soon available</div>
            <div v-if="integration.beta" class="integration-tag gray">beta</div>
        </router-link>
      </div>
      <div class="integration-box">
        <a href="https://github.com/metriql/metriql/issues" target="_blank">
          <i class="el-icon el-icon-link" style="position:absolute;right: 10px;top:10px" />

          <span class="integration-logo">
              Request integration</span>
        </a>
      </div>
    </div>
    <div v-else class="integration-content-box" :class="{full: activeIntegration.full}">
      <el-page-header @back="$router.push($route.path)" :content="`Integrate ${activeIntegration.label} ${activeIntegrationSuffix}`" />
      <img v-if="activeIntegration.logo != null" :src="activeIntegration.logo" style="max-width: 200px;margin:30px"/>
      <a v-if="activeIntegration.publicDocs" :href="activeIntegration.publicDocs" target="_blank" style="position:absolute;right:10px;top:20px"><i class="el-icon el-icon-link" />see docs</a>

      <component :is="activeIntegration.docs" :value="activeIntegration" class="integration-content-markdown"/>
    </div>
  </main>
</template>

<script>
import integrations from '/library/integrations'
import tableauLogo from '../assets/images/integrations/tableau.png'

export default {
  name: 'Integrations',
  components: {},
  computed: {
    activeIntegrationSuffix: function() {
      if(this.activeIntegration == null) return ''
      return this.activeIntegration.beta ? '(beta)' : ''
    },
    categories: function() {
      return [null, ...new Set(integrations.map(integration => integration.category))]
    },
    activeIntegration: function () {
      let integrate = this.$route.query.integrate
      if (!integrate) return null

      return integrations.find(integration => integration.label == integrate)
    },
    integrations: function() {
      if(this.searchTerm == '' && this.activeCategory == null) {
        return integrations
      }

      const term = this.searchTerm.toLowerCase()
      return integrations.filter(integration =>
        integration.label.toLowerCase().includes(term) && (this.activeCategory == null || integration.category == this.activeCategory)
      )
    }
  },
  data () {
    return {
      searchTerm: '',
      activeCategory: null
    }
  }
}
</script>

<style lang="scss">
.integration-tag {
  z-index: 100;
  position: absolute;
  font-size: 11px;
  font-weight:bold;
  padding: 4px;
  opacity: .4;

  &.yellow {
    color: #575701;
    background: #f3f306;
  }

  &.gray {
    color: #ffffff;
    background: #acacac;
  }
}
.integration-content-box {
  background-color: var(--rkm-subnav-background-color);
  box-shadow: 0 2px 4px 0 rgb(0 0 0 / 10%);
  border-radius: var(--rkm-border-radius-base);
  position:relative;
  width: 800px;
  min-height: 300px;
  padding: 24px;
  margin: 0 auto;

  &.full {
    width: 100%;
  }
}

.integration-content-markdown {
  margin: 30px 15px;
  line-height: 40px;

  ol {
    padding: 0
  }
}

.integration-box {
  background-color: var(--rkm-subnav-background-color);
  box-shadow: 0 2px 4px 0 rgb(0 0 0 / 10%);
  border-radius: var(--rkm-border-radius-small);
  height: 120px;
  text-align:center;
  width:18.4%;
  margin: 10px 2% 10px 0;
  position: relative;
  float:left;

  &.not-ready {
    pointer-events:none;
    opacity:.6;
  }

  &:nth-of-type(5n + 1) {
    margin-right:0
  }

  .integration-logo {
    position: absolute;
    max-width: 80%;
    top: 50%;
    color: #0000009e;
    font-weight: 600;
    left: 50%;
    -ms-transform: translate(-50%, -50%);
    transform: translate(-50%, -50%);
  }
}
</style>
