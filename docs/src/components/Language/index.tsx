import React, { useSyncExternalStore, type ReactNode } from "react";
import clsx from "clsx";
import styles from "./styles.module.css";

const LANGUAGES = [
  { id: "python", label: "Python" },
  { id: "rust", label: "Rust" },
] as const;

type Language = (typeof LANGUAGES)[number]["id"];

// The chosen language is the `data-language` attribute of <html>. A script in the page head
// (docusaurus.config.ts) sets it from the reader's stored choice before anything is drawn, and
// the stylesheet hides the other language. The attribute outlives a change of page, so no page
// shows one language and then switches to the other.
const STORAGE_KEY = "raio.language";
const CHANGE_EVENT = "raio-language-change";

function subscribe(onChange: () => void): () => void {
  window.addEventListener(CHANGE_EVENT, onChange);
  return () => window.removeEventListener(CHANGE_EVENT, onChange);
}

function chosenLanguage(): Language {
  return document.documentElement.dataset.language === "rust" ? "rust" : "python";
}

function choose(language: Language): void {
  document.documentElement.dataset.language = language;
  window.localStorage.setItem(STORAGE_KEY, language);
  window.dispatchEvent(new Event(CHANGE_EVENT));
}

/** The buttons that choose the language of the code on the page. */
export function LanguageSelector(): ReactNode {
  // Only `aria-selected` comes from this value; which button looks active is the stylesheet's.
  const chosen = useSyncExternalStore<Language>(subscribe, chosenLanguage, () => "python");
  return (
    <ul className={clsx("pills", styles.selector)} role="tablist" aria-label="Language">
      {LANGUAGES.map(({ id, label }) => (
        <li
          key={id}
          role="tab"
          tabIndex={0}
          aria-selected={chosen === id}
          data-language-choice={id}
          className={clsx("pills__item", styles.item)}
          onClick={() => choose(id)}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              choose(id);
            }
          }}
        >
          {label}
        </li>
      ))}
    </ul>
  );
}

/** Content shown only when the reader chose Python. */
export function Python({ children }: { children: ReactNode }): ReactNode {
  return <div data-language-only="python">{children}</div>;
}

/** Content shown only when the reader chose Rust. */
export function Rust({ children }: { children: ReactNode }): ReactNode {
  return <div data-language-only="rust">{children}</div>;
}
