
'''
-- SQLAchemy definition of the G-NAF database tables
'''

# pylint: disable=unused-private-member, missing-class-docstring, line-too-long, invalid-name

import datetime
from sqlalchemy import String, Date, Numeric, ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass

class ADDRESS_ALIAS(Base):
    __tablename__ = 'ADDRESS_ALIAS'
    address_alias_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    principal_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_DETAIL.address_detail_pid'), nullable = True)
    alias_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_DETAIL.address_detail_pid'), nullable = True)
    alias_type_code:Mapped[str] = mapped_column(ForeignKey('ADDRESS_ALIAS_TYPE_AUT.code'), nullable = True)
    alias_comment:Mapped[str] = mapped_column(String(200), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['alias_pid'], ['ADDRESS_DETAIL.address_detail_pid'], name='ADDRESS_ALIAS_FK1'),
        ForeignKeyConstraint(['alias_type_code'], ['ADDRESS_ALIAS_TYPE_AUT.code'], name='ADDRESS_ALIAS_FK2'),
        ForeignKeyConstraint(['principal_pid'], ['ADDRESS_DETAIL.address_detail_pid'], name='ADDRESS_ALIAS_FK3'),
    )


class ADDRESS_ALIAS_TYPE_AUT(Base):
    __tablename__ = 'ADDRESS_ALIAS_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(10), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(30), nullable = True)


class ADDRESS_CHANGE_TYPE_AUT(Base):
    __tablename__ = 'ADDRESS_CHANGE_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(50), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(100), nullable = True)
    description:Mapped[str] = mapped_column(String(500), nullable = True)


class ADDRESS_DEFAULT_GEOCODE(Base):
    __tablename__ = 'ADDRESS_DEFAULT_GEOCODE'
    address_default_geocode_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    address_detail_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_DETAIL.address_detail_pid'), nullable = True)
    geocode_type_code:Mapped[str] = mapped_column(ForeignKey('GEOCODE_TYPE_AUT.code'), nullable = True)
    longitude:Mapped[float] = mapped_column(Numeric(11, 8), nullable = True)
    latitude:Mapped[float] = mapped_column(Numeric(10, 8), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['address_detail_pid'], ['ADDRESS_DETAIL.address_detail_pid'], name='ADDRESS_DEFAULT_GEOCODE_FK1'),
        ForeignKeyConstraint(['geocode_type_code'], ['GEOCODE_TYPE_AUT.code'], name='ADDRESS_DEFAULT_GEOCODE_FK2'),
    )


class ADDRESS_DETAIL(Base):
    __tablename__ = 'ADDRESS_DETAIL'
    address_detail_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_last_modified:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    building_name:Mapped[str] = mapped_column(String(200), nullable = True)
    lot_number_prefix:Mapped[str] = mapped_column(String(2), nullable = True)
    lot_number:Mapped[str] = mapped_column(String(5), nullable = True)
    lot_number_suffix:Mapped[str] = mapped_column(String(2), nullable = True)
    flat_type_code:Mapped[str] = mapped_column(ForeignKey('FLAT_TYPE_AUT.code'), nullable = True)
    flat_number_prefix:Mapped[str] = mapped_column(String(2), nullable = True)
    flat_number:Mapped[int] = mapped_column(Numeric(5), nullable = True)
    flat_number_suffix:Mapped[str] = mapped_column(String(2), nullable = True)
    level_type_code:Mapped[str] = mapped_column(ForeignKey('LEVEL_TYPE_AUT.code'), nullable = True)
    level_number_prefix:Mapped[str] = mapped_column(String(2), nullable = True)
    level_number:Mapped[int] = mapped_column(Numeric(3), nullable = True)
    level_number_suffix:Mapped[str] = mapped_column(String(2), nullable = True)
    number_first_prefix:Mapped[str] = mapped_column(String(3), nullable = True)
    number_first:Mapped[int] = mapped_column(Numeric(6), nullable = True)
    number_first_suffix:Mapped[str] = mapped_column(String(2), nullable = True)
    number_last_prefix:Mapped[str] = mapped_column(String(3), nullable = True)
    number_last:Mapped[int] = mapped_column(Numeric(6), nullable = True)
    number_last_suffix:Mapped[str] = mapped_column(String(2), nullable = True)
    street_locality_pid:Mapped[str] = mapped_column(ForeignKey('STREET_LOCALITY.street_locality_pid'), nullable = True)
    location_description:Mapped[str] = mapped_column(String(45), nullable = True)
    locality_pid:Mapped[str] = mapped_column(ForeignKey('LOCALITY.locality_pid'), nullable = True)
    alias_principal:Mapped[str] = mapped_column(String(1), nullable = True)
    postcode:Mapped[str] = mapped_column(String(4), nullable = True)
    private_street:Mapped[str] = mapped_column(String(75), nullable = True)
    legal_parcel_id:Mapped[str] = mapped_column(String(20), nullable = True)
    confidence:Mapped[int] = mapped_column(Numeric(1), nullable = True)
    address_site_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_SITE.address_site_pid'), nullable = True)
    level_geocoded_code:Mapped[int] = mapped_column(ForeignKey('GEOCODED_LEVEL_TYPE_AUT.code'), nullable = True)
    property_pid:Mapped[str] = mapped_column(String(15), nullable = True)
    gnaf_property_pid:Mapped[str] = mapped_column(String(15), nullable = True)
    primary_secondary:Mapped[str] = mapped_column(String(1), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['address_site_pid'], ['ADDRESS_SITE.address_site_pid'], name='ADDRESS_DETAIL_FK1'),
        ForeignKeyConstraint(['flat_type_code'], ['FLAT_TYPE_AUT.code'], name='ADDRESS_DETAIL_FK2'),
        ForeignKeyConstraint(['level_geocoded_code'], ['GEOCODED_LEVEL_TYPE_AUT.code'], name='ADDRESS_DETAIL_FK3'),
        ForeignKeyConstraint(['level_type_code'], ['LEVEL_TYPE_AUT.code'], name='ADDRESS_DETAIL_FK4'),
        ForeignKeyConstraint(['locality_pid'], ['LOCALITY.locality_pid'], name='ADDRESS_DETAIL_FK5'),
        ForeignKeyConstraint(['street_locality_pid'], ['STREET_LOCALITY.street_locality_pid'], name='ADDRESS_DETAIL_FK6'),
    )


class ADDRESS_FEATURE(Base):
    __tablename__ = 'ADDRESS_FEATURE'
    address_feature_id:Mapped[str] = mapped_column(String(16), primary_key = True, autoincrement = False)
    address_feature_pid:Mapped[str] = mapped_column(String(16), nullable = True)
    address_detail_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_DETAIL.address_detail_pid'), nullable = True)
    date_address_detail_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_address_detail_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    address_change_type_code:Mapped[str] = mapped_column(ForeignKey('ADDRESS_CHANGE_TYPE_AUT.code'), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['address_change_type_code'], ['ADDRESS_CHANGE_TYPE_AUT.code'], name='ADDRESS_FEATURE_FK1'),
        ForeignKeyConstraint(['address_detail_pid'], ['ADDRESS_DETAIL.address_detail_pid'], name='ADDRESS_FEATURE_FK2'),
    )


class ADDRESS_MESH_BLOCK_2011(Base):
    __tablename__ = 'ADDRESS_MESH_BLOCK_2011'
    address_mesh_block_2011_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    address_detail_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_DETAIL.address_detail_pid'), nullable = True)
    mb_match_code:Mapped[str] = mapped_column(ForeignKey('MB_MATCH_CODE_AUT.code'), nullable = True)
    mb_2011_pid:Mapped[str] = mapped_column(ForeignKey('MB_2011.mb_2011_pid'), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['address_detail_pid'], ['ADDRESS_DETAIL.address_detail_pid'], name='ADDRESS_MESH_BLOCK_2011_FK1'),
        ForeignKeyConstraint(['mb_2011_pid'], ['MB_2011.mb_2011_pid'], name='ADDRESS_MESH_BLOCK_2011_FK2'),
        ForeignKeyConstraint(['mb_match_code'], ['MB_MATCH_CODE_AUT.code'], name='ADDRESS_MESH_BLOCK_2011_FK3'),
    )


class ADDRESS_MESH_BLOCK_2016(Base):
    __tablename__ = 'ADDRESS_MESH_BLOCK_2016'
    address_mesh_block_2016_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    address_detail_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_DETAIL.address_detail_pid'), nullable = True)
    mb_match_code:Mapped[str] = mapped_column(ForeignKey('MB_MATCH_CODE_AUT.code'), nullable = True)
    mb_2016_pid:Mapped[str] = mapped_column(ForeignKey('MB_2016.mb_2016_pid'), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['address_detail_pid'], ['ADDRESS_DETAIL.address_detail_pid'], name='ADDRESS_MESH_BLOCK_2016_FK1'),
        ForeignKeyConstraint(['mb_2016_pid'], ['MB_2016.mb_2016_pid'], name='ADDRESS_MESH_BLOCK_2016_FK2'),
        ForeignKeyConstraint(['mb_match_code'], ['MB_MATCH_CODE_AUT.code'], name='ADDRESS_MESH_BLOCK_2016_FK3'),
    )


class ADDRESS_SITE(Base):
    __tablename__ = 'ADDRESS_SITE'
    address_site_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    address_type:Mapped[str] = mapped_column(ForeignKey('ADDRESS_TYPE_AUT.code'), nullable = True)
    address_site_name:Mapped[str] = mapped_column(String(200), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['address_type'], ['ADDRESS_TYPE_AUT.code'], name='ADDRESS_SITE_FK1'),
    )


class ADDRESS_SITE_GEOCODE(Base):
    __tablename__ = 'ADDRESS_SITE_GEOCODE'
    address_site_geocode_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    address_site_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_SITE.address_site_pid'), nullable = True)
    geocode_site_name:Mapped[str] = mapped_column(String(200), nullable = True)
    geocode_site_description:Mapped[str] = mapped_column(String(45), nullable = True)
    geocode_type_code:Mapped[str] = mapped_column(ForeignKey('GEOCODE_TYPE_AUT.code'), nullable = True)
    reliability_code:Mapped[int] = mapped_column(ForeignKey('GEOCODE_RELIABILITY_AUT.code'), nullable = True)
    boundary_extent:Mapped[int] = mapped_column(Numeric(7), nullable = True)
    planimetric_accuracy:Mapped[int] = mapped_column(Numeric(12), nullable = True)
    elevation:Mapped[int] = mapped_column(Numeric(7), nullable = True)
    longitude:Mapped[float] = mapped_column(Numeric(11, 8), nullable = True)
    latitude:Mapped[float] = mapped_column(Numeric(10, 8), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['address_site_pid'], ['ADDRESS_SITE.address_site_pid'], name='ADDRESS_SITE_GEOCODE_FK1'),
        ForeignKeyConstraint(['geocode_type_code'], ['GEOCODE_TYPE_AUT.code'], name='ADDRESS_SITE_GEOCODE_FK2'),
        ForeignKeyConstraint(['reliability_code'], ['GEOCODE_RELIABILITY_AUT.code'], name='ADDRESS_SITE_GEOCODE_FK3'),
    )


class ADDRESS_TYPE_AUT(Base):
    __tablename__ = 'ADDRESS_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(8), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(30), nullable = True)


class FLAT_TYPE_AUT(Base):
    __tablename__ = 'FLAT_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(7), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(30), nullable = True)


class GEOCODED_LEVEL_TYPE_AUT(Base):
    __tablename__ = 'GEOCODED_LEVEL_TYPE_AUT'
    code:Mapped[int] = mapped_column(Numeric(2), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(70), nullable = True)


class GEOCODE_RELIABILITY_AUT(Base):
    __tablename__ = 'GEOCODE_RELIABILITY_AUT'
    code:Mapped[int] = mapped_column(Numeric(1), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(100), nullable = True)


class GEOCODE_TYPE_AUT(Base):
    __tablename__ = 'GEOCODE_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(4), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(250), nullable = True)


class LEVEL_TYPE_AUT(Base):
    __tablename__ = 'LEVEL_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(4), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(30), nullable = True)


class LOCALITY(Base):
    __tablename__ = 'LOCALITY'
    locality_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    locality_name:Mapped[str] = mapped_column(String(100), nullable = True)
    primary_postcode:Mapped[str] = mapped_column(String(4), nullable = True)
    locality_class_code:Mapped[str] = mapped_column(ForeignKey('LOCALITY_CLASS_AUT.code'), nullable = True)
    state_pid:Mapped[str] = mapped_column(ForeignKey('STATE.state_pid'), nullable = True)
    gnaf_locality_pid:Mapped[str] = mapped_column(String(15), nullable = True)
    gnaf_reliability_code:Mapped[int] = mapped_column(ForeignKey('GEOCODE_RELIABILITY_AUT.code'), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['gnaf_reliability_code'], ['GEOCODE_RELIABILITY_AUT.code'], name='LOCALITY_FK1'),
        ForeignKeyConstraint(['locality_class_code'], ['LOCALITY_CLASS_AUT.code'], name='LOCALITY_FK2'),
        ForeignKeyConstraint(['state_pid'], ['STATE.state_pid'], name='LOCALITY_FK3'),
    )


class LOCALITY_ALIAS(Base):
    __tablename__ = 'LOCALITY_ALIAS'
    locality_alias_pid:Mapped[str] = mapped_column(String(15), primary_key=True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    locality_pid:Mapped[str] = mapped_column(ForeignKey('LOCALITY.locality_pid'), nullable = True)
    name:Mapped[str] = mapped_column(String(100), nullable = True)
    postcode:Mapped[str] = mapped_column(String(4), nullable = True)
    alias_type_code:Mapped[str] = mapped_column(ForeignKey('LOCALITY_ALIAS_TYPE_AUT.code'), nullable = True)
    state_pid:Mapped[str] = mapped_column(String(15), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['alias_type_code'], ['LOCALITY_ALIAS_TYPE_AUT.code'], name='LOCALITY_ALIAS_FK1'),
        ForeignKeyConstraint(['locality_pid'], ['LOCALITY.locality_pid'], name='LOCALITY_ALIAS_FK2'),
    )


class LOCALITY_ALIAS_TYPE_AUT(Base):
    __tablename__ = 'LOCALITY_ALIAS_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(10), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(100), nullable = True)


class LOCALITY_CLASS_AUT(Base):
    __tablename__ = 'LOCALITY_CLASS_AUT'
    code:Mapped[str] = mapped_column(String(1), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(200), nullable = True)


class LOCALITY_NEIGHBOUR(Base):
    __tablename__ = 'LOCALITY_NEIGHBOUR'
    locality_neighbour_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    locality_pid:Mapped[str] = mapped_column(ForeignKey('LOCALITY.locality_pid'), nullable = True)
    neighbour_locality_pid:Mapped[str] = mapped_column(ForeignKey('LOCALITY.locality_pid'), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['locality_pid'], ['LOCALITY.locality_pid'], name='LOCALITY_NEIGHBOUR_FK1'),
        ForeignKeyConstraint(['neighbour_locality_pid'], ['LOCALITY.locality_pid'], name='LOCALITY_NEIGHBOUR_FK2'),
    )


class LOCALITY_POINT(Base):
    __tablename__ = 'LOCALITY_POINT'
    locality_point_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    locality_pid:Mapped[str] = mapped_column(ForeignKey('LOCALITY.locality_pid'), nullable = True)
    planimetric_accuracy:Mapped[int] = mapped_column(Numeric(12), nullable = True)
    longitude:Mapped[float] = mapped_column(Numeric(11, 8), nullable = True)
    latitude:Mapped[float] = mapped_column(Numeric(10, 8), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['locality_pid'], ['LOCALITY.locality_pid'], name='LOCALITY_POINT_FK1'),
    )


class MB_2011(Base):
    __tablename__ = 'MB_2011'
    mb_2011_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    mb_2011_code:Mapped[str] = mapped_column(String(15), nullable = True)


class MB_2016(Base):
    __tablename__ = 'MB_2016'
    mb_2016_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    mb_2016_code:Mapped[str] = mapped_column(String(15), nullable = True)


class MB_MATCH_CODE_AUT(Base):
    __tablename__ = 'MB_MATCH_CODE_AUT'
    code:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(100), nullable = True)
    description:Mapped[str] = mapped_column(String(250), nullable = True)


class PRIMARY_SECONDARY(Base):
    __tablename__ = 'PRIMARY_SECONDARY'
    primary_secondary_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    primary_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_DETAIL.address_detail_pid'), nullable = True)
    secondary_pid:Mapped[str] = mapped_column(ForeignKey('ADDRESS_DETAIL.address_detail_pid'), nullable = True)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    ps_join_type_code:Mapped[int] = mapped_column(ForeignKey('PS_JOIN_TYPE_AUT.code'), nullable = True)
    ps_join_comment:Mapped[str] = mapped_column(String(500), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['primary_pid'], ['ADDRESS_DETAIL.address_detail_pid'], name='PRIMARY_SECONDARY_FK1'),
        ForeignKeyConstraint(['ps_join_type_code'], ['PS_JOIN_TYPE_AUT.code'], name='PRIMARY_SECONDARY_FK2'),
        ForeignKeyConstraint(['secondary_pid'], ['ADDRESS_DETAIL.address_detail_pid'], name='PRIMARY_SECONDARY_FK3'),
    )


class PS_JOIN_TYPE_AUT(Base):
    __tablename__ = 'PS_JOIN_TYPE_AUT'
    code:Mapped[int] = mapped_column(Numeric(2), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(500), nullable = True)


class STATE(Base):
    __tablename__ = 'STATE'
    state_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    state_name:Mapped[str] = mapped_column(String(50), nullable = True)
    state_abbreviation:Mapped[str] = mapped_column(String(3), nullable = True)


class STREET_CLASS_AUT(Base):
    __tablename__ = 'STREET_CLASS_AUT'
    code:Mapped[str] = mapped_column(String(1), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(200), nullable = True)


class STREET_LOCALITY(Base):
    __tablename__ = 'STREET_LOCALITY'
    street_locality_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    street_class_code:Mapped[str] = mapped_column(ForeignKey('STREET_CLASS_AUT.code'), nullable = True)
    street_name:Mapped[str] = mapped_column(String(100), nullable = True)
    street_type_code:Mapped[str] = mapped_column(ForeignKey('STREET_TYPE_AUT.code'), nullable = True)
    street_suffix_code:Mapped[str] = mapped_column(ForeignKey('STREET_SUFFIX_AUT.code'), nullable = True)
    locality_pid:Mapped[str] = mapped_column(ForeignKey('LOCALITY.locality_pid'), nullable = True)
    gnaf_street_pid:Mapped[str] = mapped_column(String(15), nullable = True)
    gnaf_street_confidence:Mapped[int] = mapped_column(Numeric(1), nullable = True)
    gnaf_reliability_code:Mapped[int] = mapped_column(ForeignKey('GEOCODE_RELIABILITY_AUT.code'), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['gnaf_reliability_code'], ['GEOCODE_RELIABILITY_AUT.code'], name='STREET_LOCALITY_FK1'),
        ForeignKeyConstraint(['locality_pid'], ['LOCALITY.locality_pid'], name='STREET_LOCALITY_FK2'),
        ForeignKeyConstraint(['street_class_code'], ['STREET_CLASS_AUT.code'], name='STREET_LOCALITY_FK3'),
        ForeignKeyConstraint(['street_suffix_code'], ['STREET_SUFFIX_AUT.code'], name='STREET_LOCALITY_FK4'),
        ForeignKeyConstraint(['street_type_code'], ['STREET_TYPE_AUT.code'], name='STREET_LOCALITY_FK5'),
    )


class STREET_LOCALITY_ALIAS(Base):
    __tablename__ = 'STREET_LOCALITY_ALIAS'
    street_locality_alias_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    street_locality_pid:Mapped[str] = mapped_column(ForeignKey('STREET_LOCALITY.street_locality_pid'), nullable = True)
    street_name:Mapped[str] = mapped_column(String(100), nullable = True)
    street_type_code:Mapped[str] = mapped_column(ForeignKey('STREET_TYPE_AUT.code'), nullable = True)
    street_suffix_code:Mapped[str] = mapped_column(ForeignKey('STREET_SUFFIX_AUT.code'), nullable = True)
    alias_type_code:Mapped[str] = mapped_column(ForeignKey('STREET_LOCALITY_ALIAS_TYPE_AUT.code'), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['alias_type_code'], ['STREET_LOCALITY_ALIAS_TYPE_AUT.code'], name='STREET_LOCALITY_ALIAS_FK1'),
        ForeignKeyConstraint(['street_locality_pid'], ['STREET_LOCALITY.street_locality_pid'], name='STREET_LOCALITY_ALIAS_FK2'),
        ForeignKeyConstraint(['street_suffix_code'], ['STREET_SUFFIX_AUT.code'], name='STREET_LOCALITY_ALIAS_FK3'),
        ForeignKeyConstraint(['street_type_code'], ['STREET_TYPE_AUT.code'], name='STREET_LOCALITY_ALIAS_FK4'),
    )


class STREET_LOCALITY_ALIAS_TYPE_AUT(Base):
    __tablename__ = 'STREET_LOCALITY_ALIAS_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(10), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(15), nullable = True)


class STREET_LOCALITY_POINT(Base):
    __tablename__ = 'STREET_LOCALITY_POINT'
    street_locality_point_pid:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    date_created:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    date_retired:Mapped[datetime.date] = mapped_column(Date, nullable = True)
    street_locality_pid:Mapped[str] = mapped_column(ForeignKey('STREET_LOCALITY.street_locality_pid'), nullable = True)
    boundary_extent:Mapped[int] = mapped_column(Numeric(7), nullable = True)
    planimetric_accuracy:Mapped[int] = mapped_column(Numeric(12), nullable = True)
    longitude:Mapped[float] = mapped_column(Numeric(11, 8), nullable = True)
    latitude:Mapped[float] = mapped_column(Numeric(10, 8), nullable = True)
    __table_args__ = (
        ForeignKeyConstraint(['street_locality_pid'], ['STREET_LOCALITY.street_locality_pid'], name='STREET_LOCALITY_POINT_FK1'),
    )


class STREET_SUFFIX_AUT(Base):
    __tablename__ = 'STREET_SUFFIX_AUT'
    code:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(30), nullable = True)


class STREET_TYPE_AUT(Base):
    __tablename__ = 'STREET_TYPE_AUT'
    code:Mapped[str] = mapped_column(String(15), primary_key = True, autoincrement = False)
    name:Mapped[str] = mapped_column(String(50), nullable = True)
    description:Mapped[str] = mapped_column(String(15), nullable = True)
