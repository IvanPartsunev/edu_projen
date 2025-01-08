from projen.awscdk import AwsCdkPythonApp

project = AwsCdkPythonApp(
    author_email="ivan.parcunev@gmail.com",
    author_name="IvanPartsunev",
    cdk_version="2.1.0",
    module_name="edu_projen_project",
    name="my_project",
    version="0.1.0",
)

project.synth()