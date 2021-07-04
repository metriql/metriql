import vue from '@vitejs/plugin-vue'
import Markdown from 'vite-plugin-md'
/**
 * @type {import('vite').UserConfig}
 */
export default {
  base: '/ui',
  plugins: [vue(
    {
      include: [/\.vue$/, /\.md$/ ], // <--
    }
  ), Markdown()]
}
