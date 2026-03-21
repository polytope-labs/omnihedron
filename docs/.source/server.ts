// @ts-nocheck
import * as __fd_glob_15 from "../content/docs/testing.mdx?collection=docs"
import * as __fd_glob_14 from "../content/docs/sql-patterns.mdx?collection=docs"
import * as __fd_glob_13 from "../content/docs/schema-generation.mdx?collection=docs"
import * as __fd_glob_12 from "../content/docs/performance.mdx?collection=docs"
import * as __fd_glob_11 from "../content/docs/naming-conventions.mdx?collection=docs"
import * as __fd_glob_10 from "../content/docs/migration.mdx?collection=docs"
import * as __fd_glob_9 from "../content/docs/installation.mdx?collection=docs"
import * as __fd_glob_8 from "../content/docs/index.mdx?collection=docs"
import * as __fd_glob_7 from "../content/docs/how-it-works.mdx?collection=docs"
import * as __fd_glob_6 from "../content/docs/historical-queries.mdx?collection=docs"
import * as __fd_glob_5 from "../content/docs/docker.mdx?collection=docs"
import * as __fd_glob_4 from "../content/docs/deployment.mdx?collection=docs"
import * as __fd_glob_3 from "../content/docs/configuration.mdx?collection=docs"
import * as __fd_glob_2 from "../content/docs/changelog.mdx?collection=docs"
import * as __fd_glob_1 from "../content/docs/api-reference.mdx?collection=docs"
import { default as __fd_glob_0 } from "../content/docs/meta.json?collection=docs"
import { server } from 'fumadocs-mdx/runtime/server';
import type * as Config from '../source.config';

const create = server<typeof Config, import("fumadocs-mdx/runtime/types").InternalTypeConfig & {
  DocData: {
  }
}>({"doc":{"passthroughs":["extractedReferences"]}});

export const docs = await create.docs("docs", "content/docs", {"meta.json": __fd_glob_0, }, {"api-reference.mdx": __fd_glob_1, "changelog.mdx": __fd_glob_2, "configuration.mdx": __fd_glob_3, "deployment.mdx": __fd_glob_4, "docker.mdx": __fd_glob_5, "historical-queries.mdx": __fd_glob_6, "how-it-works.mdx": __fd_glob_7, "index.mdx": __fd_glob_8, "installation.mdx": __fd_glob_9, "migration.mdx": __fd_glob_10, "naming-conventions.mdx": __fd_glob_11, "performance.mdx": __fd_glob_12, "schema-generation.mdx": __fd_glob_13, "sql-patterns.mdx": __fd_glob_14, "testing.mdx": __fd_glob_15, });