"use strict";

$(document).ready(function() {
    var projects;

    var set_options = function(target, options) {
        target.empty();
        $.each(options, function(index, option) {
            console.log(option);
            $('<option/>').val(option.value).text(option.text).appendTo(target);
        });
    }

    var setup_importers = function(data) {
        projects = data['projects'];
        var elem = $("#importer");

        set_options(elem, _.map(projects, function(val) {
            return {
                'value': val.slug,
                'text': val.slug
            };
        }));
    }

    var setup = function() {
        $.ajax({
            method: 'GET',
            dataType: 'json',
            url: window.bpa_workflow_config['metadata_endpoint'],
        }).done(function(result) {
            setup_importers(result);
        });
    };

    setup();

});
