// @ts-nocheck
import { browser } from 'fumadocs-mdx/runtime/browser';
import type * as Config from '../source.config';

const create = browser<typeof Config, import("fumadocs-mdx/runtime/types").InternalTypeConfig & {
  DocData: {
  }
}>();
const browserCollections = {
  docs: create.doc("docs", {"api-reference.mdx": () => import("../content/docs/api-reference.mdx?collection=docs"), "changelog.mdx": () => import("../content/docs/changelog.mdx?collection=docs"), "configuration.mdx": () => import("../content/docs/configuration.mdx?collection=docs"), "deployment.mdx": () => import("../content/docs/deployment.mdx?collection=docs"), "docker.mdx": () => import("../content/docs/docker.mdx?collection=docs"), "historical-queries.mdx": () => import("../content/docs/historical-queries.mdx?collection=docs"), "how-it-works.mdx": () => import("../content/docs/how-it-works.mdx?collection=docs"), "index.mdx": () => import("../content/docs/index.mdx?collection=docs"), "installation.mdx": () => import("../content/docs/installation.mdx?collection=docs"), "migration.mdx": () => import("../content/docs/migration.mdx?collection=docs"), "naming-conventions.mdx": () => import("../content/docs/naming-conventions.mdx?collection=docs"), "performance.mdx": () => import("../content/docs/performance.mdx?collection=docs"), "schema-generation.mdx": () => import("../content/docs/schema-generation.mdx?collection=docs"), "sql-patterns.mdx": () => import("../content/docs/sql-patterns.mdx?collection=docs"), "testing.mdx": () => import("../content/docs/testing.mdx?collection=docs"), }),
};
export default browserCollections;