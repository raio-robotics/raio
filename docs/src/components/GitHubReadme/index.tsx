import { useEffect, useState, type ReactNode } from "react";
import styles from "./styles.module.css";

/** The README of a GitHub repository as GitHub renders it, loaded when the page is shown. */
export default function GitHubReadme({ repository }: { repository: string }): ReactNode {
  const [readme, setReadme] = useState<string>();
  useEffect(() => {
    const headers = { Accept: "application/vnd.github.html" };
    fetch(`https://api.github.com/repos/${repository}/readme`, { headers }).then(async (response) => {
      if (response.ok) setReadme(await response.text());
    });
  }, [repository]);
  // The page links to the README until GitHub has sent it, and keeps the link if GitHub refuses.
  if (readme === undefined) {
    return <a href={`https://github.com/${repository}#readme`}>The README of {repository} on GitHub</a>;
  }
  return <div className={styles.readme} dangerouslySetInnerHTML={{ __html: readme }} />;
}
