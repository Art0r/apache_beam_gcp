"""
Setup internal packages for project 
"""
from setuptools import find_packages, setup

setup(
    name="curso_apache_beam_gcp_ext_lib",
    version="1.0",
    install_requires=[
        "psycopg2-binary==2.9.10",
        "google-cloud-secret-manager==2.22.0",
        "apache-beam==2.61.0"
    ],
    packages=find_packages(
        include=["curso_apache_beam_gcp_ext_lib", "curso_apache_beam_gcp_ext_lib.*"]),
    zip_safe=False,
)

print("Finished Setup")
