<template>
  <div class="rkm-c-subnav">
    <ul role="menubar" class="rkm-c-subnav__menu" style="text-align:center">
      <el-tabs v-model="editableTabsValue" type="card" closable class="rkm-c-subnav__menu" @tab-remove="removeTab">
        <el-tab-pane
          v-for="(item, index) in editableTabs"
          :key="item.name"
          :label="item.title"
          :name="item.name"
        >
        </el-tab-pane>
      </el-tabs>
      <el-button
        size="small"
        @click="addTab(editableTabsValue)"
      >
        add tab
      </el-button>
      <!--      <li-->
      <!--        v-for="section in sections"-->
      <!--        :key="section.id"-->
      <!--        role="none"-->
      <!--        class="rkm-c-subnav__item"-->
      <!--        :class="{ 'is-active': (true === section.id) }">-->
      <!--        <a role="menuitem"-->
      <!--          href="#"-->
      <!--          class="rkm-c-subnav__link"-->
      <!--          @click.prevent="$emit('select-section', section.id)">-->
      <!--          {{ section.label }}-->
      <!--        </a>-->
      <!--      </li>-->
    </ul>

<!--    <el-button type="primary">Execute</el-button>-->
  </div>
</template>

<script>
export default {
  name: 'AppSubnav',
  props: {
    sections: {
      type: Array,
      required: true,
      validator (sections) {
        // Only valid if the id, label and pageId props exists
        return sections.every((section) => section.id && section.label && section.pageId)
      }
    }
  },
  data() {
    return {
      editableTabsValue: '2',
      editableTabs: [{
        title: 'Tab 1',
        name: '1',
        content: 'Tab 1 content'
      }, {
        title: 'Tab 2',
        name: '2',
        content: 'Tab 2 content'
      }],
      tabIndex: 2
    }
  },
  methods: {
    addTab(targetName) {
      let newTabName = ++this.tabIndex + '';
      this.editableTabs.push({
        title: 'New Tab',
        name: newTabName,
        content: 'New Tab content'
      });
      this.editableTabsValue = newTabName;
    },
    removeTab(targetName) {
      let tabs = this.editableTabs;
      let activeName = this.editableTabsValue;
      if (activeName === targetName) {
        tabs.forEach((tab, index) => {
          if (tab.name === targetName) {
            let nextTab = tabs[index + 1] || tabs[index - 1];
            if (nextTab) {
              activeName = nextTab.name;
            }
          }
        });
      }

      this.editableTabsValue = activeName;
      this.editableTabs = tabs.filter(tab => tab.name !== targetName);
    }
  }
}
</script>

<style lang="scss">
.rkm-c-subnav {
  display: flex;
  background-color: var(--rkm-subnav-background-color);
  box-shadow: 0 2px 4px 0 rgba(0, 0, 0, 0.1);

  &__menu {
    display: flex;
    flex-flow: row wrap;
    list-style-type: none;
    margin: 0;
    padding: 0 0 0 50px;
  }

  &__item {
    padding: 24px 32px;
    color: var(--rkm-subnav-link-color);
    font-weight: var(--rkm-font-weight-medium);

    &.is-active {
      color: var(--rkm-subnav-active-link-color);
      border-bottom: 1px solid var(--rkm-subnav-active-border-color);
      font-weight: var(--rkm-font-weight-bold);
    }
  }

  &__link {
    color: inherit;
    font-size: var(--rkm-small);

    :hover {
      color: var(--rkm-subnav-hover-link-color);
    }
  }
}
</style>
