Name:           libcastle
Version:        %{buildver}
Release:        %{buildrev}
Summary:        C library for castle fs

Group:          Filesystem
License:        MIT
Source0:        %{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Provides:       %{name}-%{changesetver}

%description

%package devel
Group:          Development/Filesystems
Summary:        Development files for libcastle
Requires:       %{name} = %{version}-%{release}

%description devel

%prep
%setup -q -n %{name}

%build
make all

%install
rm -rf %{buildroot}

export DESTDIR=%{buildroot}
export DONT_RUN_LDCONFIG=y
make install 

%clean
rm -rf %{buildroot}

%post
/sbin/ldconfig

%postun
/sbin/ldconfig

%files
%defattr(-,root,root,-)
/usr/lib64/libcastle.so.1

%files devel
/usr/lib64/libcastle.so
/usr/include/castle/*

%changelog
* Mon Oct 18 2010 Andrew Suffield <asuffield@acunu.com> - %{buildver}-%{buildrev}
- Initial packaging
