"use strict";

$(document).ready(function() {
    var projects;
    var importers;
    var blank_option = {'text': '----', 'value': null};

    var filesList = [];
    var paramNames = [];
    
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

    var validate_ui = function() {
        var ok = false;
        if ($("#project").val() && $("#importer").val() && filesList.length == 2) {
            ok = true;
        }
        if (ok) {
            $('#verify-btn').removeProp('disabled');
        } else {
            $('#verify-btn').prop('disabled', 'true');
        }
    }

    var reset_ui = function() {
        var form = $("#verify-form")[0].reset();
        $.each(['md5', 'xlsx'], function(i, t) {
            $('#' + t + '-file').show();
            $('#' + t + '-done').hide();
        });
        filesList.length = 0;
        paramNames.length = 0;
        validate_ui();
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
            validate_ui();
        });
        $("#importer").on('change', function() {
            validate_ui();
        });
        reset_ui();
    };
    
    setup();
    
    $("#verify-form").fileupload({
        url: window.bpa_workflow_config['validate_endpoint'],
        type: 'POST',
        dataType: 'json',
        autoUpload: false,
        singleFileUploads: false,
        formData: {},
        
        add: function(e, data) {
            var target = e.delegatedEvent.target.name;
            var filename = data.files[0].name;

            filesList.push(data.files[0]);
            paramNames.push(target);

            $("#" + target + "-file").hide()
            $("#" + target + "-done").empty();
            $("#" + target + "-done").show();
            $("<p>").text(filename).appendTo($("#" + target + "-done"));

            validate_ui();
            
            return false;
        }
    });

    $('#reset-btn').click(function (e) {
        e.preventDefault();
        reset_ui();
    });
    
    $('#verify-btn').click(function (e) {
        console.log(filesList);
        console.log(paramNames);
        e.preventDefault();
        $("#verify-form").fileupload('send', {
            files: filesList,
            formData: $("#verify-form").serializeArray()
        });
    });
});
