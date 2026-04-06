// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

const site = process.env.SITE_URL ?? "http://localhost:4321";
const base = process.env.BASE_PATH ?? "/";
const basePrefix = base.endsWith("/") ? base : `${base}/`;

export default defineConfig({
  site,
  base,
  integrations: [
    starlight({
      title: "ELLIIIT Dataset Docs",
      favicon: `${basePrefix}elliiit-favicon.svg`,
      logo: {
        light: "./src/assets/elliiit-logo-light.svg",
        dark: "./src/assets/elliiit-logo-dark.svg",
        alt: "ELLIIIT Dataset Docs",
      },
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/techtile-by-dramco/ELLIIIT-dataset-26",
        },
      ],
      sidebar: [
        {
          label: "Tutorials",
          items: [
            { label: "Getting Started", slug: "tutorials/getting-started" },
            { label: "Use the RF Xarray", slug: "tutorials/build-rf-xarray" },
            { label: "Notebook: RF Xarray Structure", slug: "tutorials/notebook-xarray-structure" },
            { label: "Notebook: Acoustic Xarray Structure", slug: "tutorials/notebook-acoustic-xarray-structure" },
            { label: "Notebook: RF Dataset Overview", slug: "tutorials/notebook-overview" },
            { label: "Notebook: Rover Positions", slug: "tutorials/notebook-rover-positions" },
            { label: "Notebook: CSI Per Position", slug: "tutorials/notebook-csi-per-position" },
            { label: "Notebook: RF And Acoustic At One Position", slug: "tutorials/notebook-rf-acoustic-position" },
            { label: "Notebook: CSI Movies", slug: "tutorials/notebook-csi-movies" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "Techtile Background", slug: "reference/techtile-background" },
            { label: "Measurement Setup", slug: "reference/measurement-setup" },
            { label: "Measurement Sequence", slug: "reference/measurement-sequence" },
            { label: "Data Products", slug: "reference/data-products" },
            { label: "Interpretation and Joins", slug: "reference/interpretation-and-joins" },
            { label: "Runtime Architecture", slug: "reference/runtime-architecture" },
            { label: "Configuration", slug: "reference/configuration" },
            { label: "Deployment and Acquisition", slug: "reference/deployment-and-acquisition" },
            { label: "RF Post-Processing", slug: "reference/rf-post-processing" },
            { label: "Operational Utilities", slug: "reference/operational-utilities" },
          ],
        },
      ],
    }),
  ],
});
