import MDXComponents from "@theme-original/MDXComponents";
import { LanguageSelector, Python, Rust } from "@site/src/components/Language";

// Makes the language components available in every page without an import.
export default {
  ...MDXComponents,
  LanguageSelector,
  Python,
  Rust,
};
