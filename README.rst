Welcome to "Dynamodb Modeler" Project
==============================================================================

.. image:: https://user-images.githubusercontent.com/6800411/212983034-b0d8f048-228e-4be6-b591-1e39a70d64ec.png

This project presents a DynamoDB modeling tool designed as a sandbox for developers to define DynamoDB data models tailored to specific business scenarios. Developers can experiment with business logic, user interactions, and query patterns, enabling the creation of robust and scalable data models.

The tool empowers developers to explore various data modeling techniques, test their assumptions, and iterate on their designs. This iterative process helps developers gain a deeper understanding of how to optimize their data models for performance, scalability, and cost-effectiveness.


How to Use
------------------------------------------------------------------------------
If you don't know common strategy to declare one-to-many and many-to-many relationship, please read this `Reinvent YouTube In DynamoDB <./Reinvent-YouTube-In-DynamoDB.ipynb>`_ notebook to Learn. You can also read the `One-To-Many-Relationship <./01-One-To-Many-Relationship/index.rst>`_ and `Many-To-Many-Relationship <./02-Many-To-Many-Relationship/index.rst>`_ for more detailed information.

If you want to design a new data model for your project, you can duplicate `Reinvent YouTube In DynamoDB <./Reinvent-YouTube-In-DynamoDB.ipynb>`_ notebook to start your own project.


More DynamoDB Modeling Examples
------------------------------------------------------------------------------
- `Reinvent-AWS-S3-in-DynamoDB <./Reinvent-AWS-S3-in-DynamoDB.ipynb>`_
- `Reinvent-Versioned-Document-System-in-DynamoDB <./reinvent_versioned_document_system_in_dynamodb.py>`_


Install Dependencies
------------------------------------------------------------------------------
.. code-block:: console

    virtualenv -p python3.10 .venv # python3.7+ is recommended

.. code-block:: console

    source .venv/bin/activate

.. code-block:: console

    pip install -r requirements.txt

.. code-block:: console

    .venv/bin/jupyter-lab
