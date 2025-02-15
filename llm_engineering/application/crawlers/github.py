import os
import shutil
import subprocess
import tempfile

from loguru import logger

from llm_engineering.domain.documents import RepositoryDocument

from .base import BaseCrawler

'''
download github repository to local directory and then crawl every file in lcoal dr
'''
class GithubCrawler(BaseCrawler):
    # model is Repository class
    model = RepositoryDocument

    def __init__(self, ignore = (".git", ".toml", ".lock", ".png")) -> None:
        super().__init__()
        self._ignore = ignore
    
    def extract(self, link: str, **kwargs) -> None:
        # NoSQLbaseDocument class implements a clas method that checks if a link already exists in MongoDB
        old_model = self.model.find(link = link)
        if old_model is not None:
            logger.info(f"Respository alrady exists in the database: {link}")

            return
        
        logger.info(f"Starting scrapping GitHub repository: {link}")

        repo_name = link.rstrip("/").split("/")[-1]

        local_temp = tempfile.mkdtemp()
    
        try:
            os.chdir(local_temp)
            subprocess.run(["git", "clone", link])

            repo_path = os.path.join(local_temp, os.listdir(local_temp)[0])

            tree = {}

            # os.walk exhausts all directories/files under the given folder
            for root, _, files in os.walk(repo_path):
                dir = root.replace(repo_path, "").lstrip("/")
                if dir.startswith(self._ignore):
                    continue

                for file in files:
                    if file.endswith(self._ignore):
                        continue
                    file_path = os.path.join(dir, file)
                    with open(os.path.join(root, file), "r", errors = "ignore") as f:
                        tree[file_path] = f.read().replace(" ", "")

            user = kwargs['user']
            instance = self.model(
                content = tree,
                name = repo_name,
                link = link,
                platform = 'github',
                author_id = user.id,
                author_full_name = user.author_full_name
            )
            instance.save()
        except Exception:
            raise
        finally:
            shutil.rmtree(local_temp)
