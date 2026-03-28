{% docs __overview__ %}

# Welcome to DbtEducationalDataProject

A comprehensive educational data analytics project built with dbt, featuring 45 interconnected models that analyze student performance, faculty effectiveness, financial metrics, and institutional operations.

## Here's what makes dbt powerful:

**Lineage Tracking** - Every transformation is fully documented — you can trace the entire data flow from the original source all the way to the dashboard or application that consumes the final (gold) data.

**Testing** - Built-in data quality checks ensure reliability. Tests in dbt are defined separately, allowing you to validate any SQL logic. If a test fails, dbt automatically prevents downstream models from building, protecting your pipeline integrity.

**Modularity** - Reusable models and macros promote the DRY (Don't Repeat Yourself) principle — you write logic once and reuse it across projects. This brings the power of functional programming concepts into data transformation.

**Documentation** - dbt automatically generates rich documentation for your entire project — including models, lineage, and tests. As you can see here, all of this was created automatically by dbt, not manually coded.

## The Limitations Wall

While dbt docs give us this lovely visual, they can't answer the complex questions our education clients ask daily. Let me give you some real examples:

🔴 **Blast Radius Analysis:**
"What's the complete blast radius if I need to rebuild the stg_students table? Show me all downstream models that would be affected, including their materialization types."

🔗 **Dependency Complexity:**
"Which models create the longest dependency chains in our project, and what's the maximum depth of any single lineage path from source to final mart?"

💰 **Resource Optimization:**
"Which source tables are contributing to the most 'table' materializations downstream? I want to optimize our warehouse storage costs."

⚙️ **Job Orchestration:**
"If I wanted to create 5 separate dbt jobs that could run in parallel, how would you group our models to minimize cross-job dependencies?"

## Why FalkorDB?

✅ **Open Source**: Truly open-source graph database
✅ **Performance**: Blazing fast for analytical queries - 496x faster than Neo4j with 6x more memory efficiency and 11x higher throughput  
✅ **Redis ecosystem**: Familiar deployment patterns built on Redis infrastructure  
✅ **Cost-effective**: More efficient for read-heavy workloads with predictable performance and lower operational costs  
✅ **Modern architecture**: Built for cloud-native environments with horizontal scaling and multi-tenancy support (10K+ graphs)  
✅ **Super suitable for AI**: Native GraphRAG capabilities with vector search, enabling precise context for LLMs and reducing hallucinations in generative AI applications  
✅ **Amazing support**: Comprehensive documentation and community support for enterprise-grade implementations

## Navigation

You can use the **Project** and **Database** navigation tabs on the left side of the window to explore the models in your project.

**Project Tab**: The Project tab mirrors the directory structure of your dbt project. In this tab, you can see all of the models defined in your dbt project, as well as models imported from dbt packages.

**Database Tab**: The Database tab also exposes your models, but in a format that looks more like a database explorer. This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are not shown in this interface, as they do not exist in the database.

## Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the **Expand** button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the `--select` and `--exclude` model selection syntax to filter the models in the graph.



```Cypher
MATCH path = (start {name: 'stg_students'})<-[:DEPENDS_ON*1..10]-(downstream)
WITH downstream, length(path) as dependency_level
RETURN DISTINCT downstream.name AS model_name, 
       downstream.materialized AS materialization_type, 
       labels(downstream) AS node_type,
       min(dependency_level) AS min_dependency_level
ORDER BY min_dependency_level, model_name
```



{% enddocs %}