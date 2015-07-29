Table Column Families
=====================

When creating a :class:`Table <gcloud_bigtable.column_family.ColumnFamily>`,
it is possible to set garbage collection rules for expired data.

By setting a rule, cells in the table matching the rule will be deleted
during periodic garbage collection (which executes opportunistically in the
background).

The types
:class:`GarbageCollectionRule <gcloud_bigtable.column_family.GarbageCollectionRule>`,
:class:`GarbageCollectionRuleUnion <gcloud_bigtable.column_family.GarbageCollectionRuleUnion>` and
:class:`GarbageCollectionRuleIntersection <gcloud_bigtable.column_family.GarbageCollectionRuleIntersection>`
can all be used as the optional ``gc_rule`` argument in the
:class:`ColumnFamily <gcloud_bigtable.column_family.ColumnFamily>`
constructor. This value is then used in the
:meth:`create <gcloud_bigtable.column_family.ColumnFamily.create>` and
:meth:`update <gcloud_bigtable.column_family.ColumnFamily.update>` methods.

These rules can be nested arbitrarily, with
:class:`GarbageCollectionRule <gcloud_bigtable.column_family.GarbageCollectionRule>`
at the lowest level of the nesting:

.. code:: python

    import datetime

    max_age = datetime.timedelta(days=3)
    rule1 = GarbageCollectionRule(max_age=max_age)
    rule2 = GarbageCollectionRule(max_num_versions=1)

    # Make a composite that matches anything older than 3 days **AND**
    # with more than 1 version.
    rule3 = GarbageCollectionIntersection(rules=[rule1, rule2])

    # Make another composite that matches our previous intersection
    # **OR** anything that has more than 3 versions.
    rule4 = GarbageCollectionRule(max_num_versions=3)
    rule5 = GarbageCollectionUnion(rules=[rule3, rule4])

Column Family Module
~~~~~~~~~~~~~~~~~~~~

.. automodule:: gcloud_bigtable.column_family
  :members:
  :undoc-members:
  :show-inheritance:
