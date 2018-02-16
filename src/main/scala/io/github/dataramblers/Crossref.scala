package io.github.dataramblers


case class CrossrefPerson(ORCID: String,
                          `authenticated-orcid`: Boolean,
                          family: String,
                          given: String,
                          name: String,
                          suffix: String
                         )

case class Crossref(DOI: String,
                    ISBN: String,
                    ISSN: String,
                    author: CrossrefPerson,
                    editor: CrossrefPerson,
                    title: String,
                    published_online: Option[Int],
                    published_print: Option[Int]
                   )
