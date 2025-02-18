from projen.awscdk import AwsCdkPythonApp

python_version = "3.12"

project = AwsCdkPythonApp(
    author_email="ivan.parcunev@gmail.com",
    author_name="IvanPartsunev",
    cdk_version="2.1.0",
    module_name="edu_projen_project",
    name="my_project",
    version="0.1.0",
    poetry=True,
    deps=[
        f"python@{python_version}.*"
    ],
    dev_deps=[
        "Boto3@^1.35.92",
        "apache-airflow@^2.10.4",
        "apache-airflow-providers-amazon@^9.2.0",
        "ray@{version = '2.40.0', extras = ['default']}",
        "pandas@^2.2.3"
    ]
)

# .gitignore:
project.gitignore.add_patterns(
    "airflow_sample/docker-compose.yaml",   #Ignore docker-compose for airflow
)

project.synth()