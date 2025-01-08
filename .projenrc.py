import os

from projen.awscdk import AwsCdkPythonApp

project = AwsCdkPythonApp(
    author_email="ivan.parcunev@gmail.com",
    author_name="IvanPartsunev",
    cdk_version="2.1.0",
    module_name="edu_projen_project",
    name="my_project",
    version="0.1.0",
    poetry=True,
)

# Dependencies:
project.add_dependency("python@~3.12")
project.add_dependency("Boto3@^1.35.92")
project.add_dependency("apache-edu_airflow@^2.10.4")

# .gitignore:
project.gitignore.add_patterns(
    "edu_airflow/docker-compose.yaml",   #Ignore docker-compose for edu_airflow
)

project.synth()