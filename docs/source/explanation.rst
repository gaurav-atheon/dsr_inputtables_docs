Explanation
===========

    - should this just be a top level page?
    - DSR architecture
    - and concepts provides understanding into
    - principles
    - facts
    - dims
    - Visibility

Visibility
----------

Delegated Visibility
~~~~~~~~~~~~~~~~~~~~
Delegated visibility allows one organisation to grant their visibility access to another organisation.
Access can be granted on a SKU level basis to individual organisations (and by definition their parents).

The method:
Input table->
Full table rebuild->
find source visibility->
dedupe existing visibility->
dedupe delegated visibility->
union into visibility (incremental???)
