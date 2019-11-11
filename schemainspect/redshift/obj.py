from collections import OrderedDict as od
from itertools import groupby

from sqlalchemy import text

from schemainspect.inspected import ColumnInfo
from schemainspect.pg.obj import InspectedSelectable as PGInspectedSelectable
from schemainspect.pg.obj import CREATE_TABLE
from schemainspect.inspector import DBInspector
from schemainspect.misc import resource_text
from schemainspect.misc import quoted_identifier
from schemainspect.pg.obj import InspectedConstraint
from schemainspect.pg.obj import InspectedFunction
from schemainspect.pg.obj import InspectedSchema

ALL_RELATIONS_QUERY = resource_text("sql/relations.sql")
SCHEMAS_QUERY = resource_text("../pg/sql/schemas.sql")
CONSTRAINTS_QUERY = resource_text("sql/constraints.sql")
FUNCTIONS_QUERY = resource_text("sql/functions.sql")
DEPS_QUERY = resource_text("sql/deps.sql")


class RedshiftInspectedSelectable(PGInspectedSelectable):
    def __init__(self, *args, diststyle="auto", dist_key=None, sort_keys, **kwargs):
        super().__init__(*args, **kwargs)
        self.diststyle = diststyle
        self.dist_key = dist_key
        self.sort_keys = sort_keys

    @property
    def create_statement(self):
        n = self.quoted_full_name
        if self.relationtype in ("r", "p"):
            colspec = ",\n".join(
                "    " + c.creation_clause for c in self.columns.values()
            )
            if colspec:
                colspec = "\n" + colspec
                create_statement = CREATE_TABLE.format(
                    n, colspec, self.table_attributes
                )
        elif self.relationtype == "v":
            create_statement = "create or replace view {} as {}\n".format(
                n, self.definition
            )
        else:
            raise NotImplementedError  # pragma: no cover
        return create_statement

    @property
    def table_attributes(self):
        dist_style = "diststyle {}".format(self.diststyle)
        return "{}\n{}{}".format(dist_style, self.distkey, self.sortkey)

    @property
    def distkey(self):
        return "distkey ({})\n".format(self.dist_key) if self.dist_key else ""

    @property
    def sortkey(self):
        if len(self.sort_keys) == 1:
            sortkey_type = ""
        elif any(order < 0 for order in self.sort_keys.values()):
            sortkey_type = "interleaved "
        else:
            sortkey_type = "compound "
        # TODO: Return COMPOUND (foo, bar, baz) etc.
        sorted_sort_keys = sorted(self.sort_keys.items(), key=lambda x: abs(x[1]))
        sorted_sortkey_names = [key[0] for key in sorted_sort_keys]
        return "{}sortkey ({})".format(sortkey_type, ", ".join(sorted_sortkey_names))


class Redshift(DBInspector):
    def __init__(self, c, include_internal=False):
        def processed(q):
            if not include_internal:
                q = q.replace("-- SKIP_INTERNAL", "")
            q = text(q)
            return q

        self.SCHEMAS_QUERY = processed(SCHEMAS_QUERY)
        self.ALL_RELATIONS_QUERY = processed(ALL_RELATIONS_QUERY)
        self.CONSTRAINTS_QUERY = processed(CONSTRAINTS_QUERY)
        self.FUNCTIONS_QUERY = processed(FUNCTIONS_QUERY)
        self.DEPS_QUERY = processed(DEPS_QUERY)
        # Postgres concepts Redshift doesn't support:
        self.extensions = {}
        self.collations = {}
        self.enums = {}
        self.sequences = {}
        self.triggers = {}
        self.rlspolicies = {}
        self.indexes = {}
        super(Redshift, self).__init__(c, include_internal=False)

    def load_all(self):
        self.load_schemas()
        self.load_all_relations()
        self.load_functions()
        self.selectables = od()
        self.selectables.update(self.relations)
        self.selectables.update(self.functions)
        self.load_deps()
        self.load_deps_all()

    def load_schemas(self):
        q = self.c.execute(self.SCHEMAS_QUERY)
        schemas = [InspectedSchema(schema=each.schema) for each in q]
        self.schemas = od((schema.schema, schema) for schema in schemas)

    def load_deps(self):
        q = self.c.execute(self.DEPS_QUERY)
        for dep in q:
            x = quoted_identifier(dep.name, dep.schema)
            x_dependent_on = quoted_identifier(
                dep.name_dependent_on,
                dep.schema_dependent_on,
                dep.identity_arguments_dependent_on,
            )
            self.selectables[x].dependent_on.append(x_dependent_on)
            self.selectables[x].dependent_on.sort()
            self.selectables[x_dependent_on].dependents.append(x)
            self.selectables[x_dependent_on].dependents.sort()

    def load_deps_all(self):
        def get_related_for_item(item, att):
            related = [self.selectables[_] for _ in getattr(item, att)]
            return [item.signature] + [
                _ for d in related for _ in get_related_for_item(d, att)
            ]

        for k, x in self.selectables.items():
            d_all = get_related_for_item(x, "dependent_on")[1:]
            d_all.sort()
            x.dependent_on_all = d_all
            d_all = get_related_for_item(x, "dependents")[1:]
            d_all.sort()
            x.dependents_all = d_all

    def load_all_relations(self):
        self.tables = od()
        self.views = od()

        q = self.c.execute(self.ALL_RELATIONS_QUERY)
        for _, g in groupby(q, lambda x: (x.relationtype, x.schema, x.name)):
            rows = list(g)
            f = rows[0]
            clist = [c for c in rows if c.position_number]

            columns = [
                ColumnInfo(
                    name=c.attname,
                    dbtype=c.datatype,
                    dbtypestr=c.datatypestring,
                    pytype=self.to_pytype(c.datatype),
                    default=c.defaultdef,
                    not_null=c.not_null,
                )
                for c in clist
            ]

            dist_key = next((c.attname for c in clist if c.is_dist_key), None)
            sort_keys = od((c.attname, c.sort_key_ord) for c in clist if c.sort_key_ord)

            s = RedshiftInspectedSelectable(
                name=f.name,
                schema=f.schema,
                columns=od((c.name, c) for c in columns),
                relationtype=f.relationtype,
                diststyle=f.diststyle,
                dist_key=dist_key,
                sort_keys=sort_keys,
                definition=f.definition,
                comment=f.comment,
            )
            RELATIONTYPES = {"r": "tables", "v": "views", "p": "tables"}
            att = getattr(self, RELATIONTYPES[f.relationtype])
            att[s.quoted_full_name] = s
        self.relations = od()
        for x in (self.tables, self.views):
            self.relations.update(x)
        q = self.c.execute(self.CONSTRAINTS_QUERY)
        constraintlist = [
            InspectedConstraint(
                name=i.name,
                schema=i.schema,
                constraint_type=i.constraint_type,
                table_name=i.table_name,
                definition=i.definition,
                index=i.index,
            )
            for i in q
        ]
        self.constraints = od((i.quoted_full_name, i) for i in constraintlist)
        # add constraints to each table
        for each in self.constraints.values():
            t = each.quoted_full_table_name
            n = each.quoted_full_name
            self.relations[t].constraints[n] = each

    def load_functions(self):
        self.functions = od()
        q = self.c.execute(self.FUNCTIONS_QUERY)
        for _, g in groupby(q, lambda x: (x.schema, x.name, x.identity_arguments)):
            clist = list(g)
            f = clist[0]
            outs = [c for c in clist if c.parameter_mode == "OUT"]
            columns = [
                ColumnInfo(
                    name=c.parameter_name,
                    dbtype=c.data_type,
                    pytype=self.to_pytype(c.data_type),
                )
                for c in outs
            ]
            if outs:
                columns = [
                    ColumnInfo(
                        name=c.parameter_name,
                        dbtype=c.data_type,
                        pytype=self.to_pytype(c.data_type),
                    )
                    for c in outs
                ]
            else:
                columns = [
                    ColumnInfo(
                        name=f.name,
                        dbtype=f.data_type,
                        pytype=self.to_pytype(f.returntype),
                        default=f.parameter_default,
                    )
                ]
            plist = [
                ColumnInfo(
                    name=c.parameter_name,
                    dbtype=c.data_type,
                    pytype=self.to_pytype(c.data_type),
                    default=c.parameter_default,
                )
                for c in clist
                if c.parameter_mode == "IN"
            ]
            s = InspectedFunction(
                schema=f.schema,
                name=f.name,
                columns=od((c.name, c) for c in columns),
                inputs=plist,
                identity_arguments=f.identity_arguments,
                result_string=f.result_string,
                language=f.language,
                definition=f.definition,
                strictness=f.strictness,
                security_type=f.security_type,
                volatility=f.volatility,
                full_definition=f.full_definition,
                comment=f.comment,
            )

            identity_arguments = "({})".format(s.identity_arguments)
            self.functions[s.quoted_full_name + identity_arguments] = s
