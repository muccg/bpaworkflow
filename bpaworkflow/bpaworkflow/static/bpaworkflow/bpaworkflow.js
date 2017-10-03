"use strict";

$(document).ready(function() {
    var projects;
    var importers;
    var blank_option = {'text': '----', 'value': null};

    var set_options = function(target, options) {
        target.empty();
        $.each(options, function(index, option) {
            $('<option/>').val(option.value).text(option.text).appendTo(target);
        });
    }

    var setup_importers = function(data) {
        projects = data['projects'];
        var elem = $("#project");

        set_options(elem, _.map(projects, function(val) {
            return {
                'value': val.name,
                'text': val.title
            };
        }));
    }

    var update_import_options = function(data) {
        var selected_project = $("#project").val();
        var options = [];
        var cap_first = function(s) {
            return s.charAt(0).toUpperCase() + s.slice(1);
        };
        if (selected_project) {
            options = _.map(importers[selected_project], function(data) {
                var desc = [];
                if (data.analysed) {
                    desc.push('Analysed');
                }
                if (data.omics) {
                    desc.push(cap_first(data.omics));
                }
                if (data.technology) {
                    desc.push(cap_first(data.technology));
                }
                if (data.pool) {
                    desc.push('(Pooled)');
                }
                return {
                    'value': data.slug,
                    'text': desc.join(' ')
                };
            });
        }
        var elem = $("#importer");
        set_options(elem, [blank_option].concat(_.sortBy(options, ['text'])));
    }

    var setup_importers = function(data) {
        projects = data['projects'];
        importers = data['importers'];
        var elem = $("#project");

        set_options(elem, [blank_option].concat(_.sortBy(_.map(projects, function(val) {
            return {
                'value': val.name,
                'text': val.title
            };
        }), ['text'])));
    }

    var setup = function() {
        $.ajax({
            method: 'GET',
            dataType: 'json',
            url: window.bpa_workflow_config['metadata_endpoint'],
        }).done(function(result) {
            setup_importers(result);
        });
        $("#project").on('change', function() {
            update_import_options();
        });
    };

    setup();

});
